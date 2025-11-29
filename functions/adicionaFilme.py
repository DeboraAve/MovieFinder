import json
import os
from datetime import datetime
from queue import Queue
from tinydb import TinyDB, Query

# Filas para simular o pipeline (SQS/SNS)
filaFilmeAdicionado = Queue()
filaNotificaAdicao = Queue()

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
USUARIO_JSON = os.path.join(BASE_DIR, 'data', 'filmeUsuario.json')
CATALOGO_JSON = os.path.join(BASE_DIR, 'data', 'filmes.json')


def _get_usuario_db():
    """
    Retorna uma instância do TinyDB para o arquivo de filmes do usuário.
    O TinyDB cria o arquivo automaticamente caso não exista.
    """
    db = TinyDB(USUARIO_JSON, ensure_ascii=False, indent=2, encoding='utf-8')
    # Garante que a tabela 'usuarios' exista
    db.table('usuarios')
    return db


def _carrega_json_resiliente(path):
    """
    Lê um arquivo JSON tentando primeiro UTF-8. Se falhar (por exemplo, arquivo
    salvo em Windows-1252), faz uma segunda tentativa em latin-1 e sobrescreve
    o arquivo em UTF-8 para evitar novos erros.
    """
    if not os.path.exists(path):
        raise FileNotFoundError(path)

    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f), 'utf-8'
    except UnicodeDecodeError:
        with open(path, 'r', encoding='latin-1') as f:
            data = json.load(f)
        # Normaliza o arquivo para UTF-8
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
        return data, 'latin-1'


def _get_catalogo_table():
    db = TinyDB(CATALOGO_JSON, ensure_ascii=False, indent=2, encoding='utf-8')
    return db.table("Filmes")

def _bootstrap_catalogo_db():
    """
    Garante que o arquivo de catálogo esteja no formato esperado pelo TinyDB.
    Se estiver no formato antigo (lista simples ou dict com chave 'filmes'),
    converte automaticamente para o formato interno do TinyDB.
    """
    if not os.path.exists(CATALOGO_JSON):
        TinyDB(CATALOGO_JSON, ensure_ascii=False, indent=2, encoding='utf-8').close()
        return

    try:
        with open(CATALOGO_JSON, 'r', encoding='utf-8') as f:
            if os.path.getsize(CATALOGO_JSON) == 0:
                raise json.JSONDecodeError('empty', '', 0)
            raw_data = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        TinyDB(CATALOGO_JSON, ensure_ascii=False, indent=2, encoding='utf-8').close()
        return

    if isinstance(raw_data, dict) and ("_default" in raw_data or "Filmes" in raw_data):
        return

    filmes = []
    if isinstance(raw_data, dict) and isinstance(raw_data.get('filmes'), list):
        filmes = raw_data['filmes']
    elif isinstance(raw_data, list):
        filmes = raw_data


    db = TinyDB(CATALOGO_JSON, ensure_ascii=False, indent=2, encoding='utf-8')
    table = db.table("Filmes")
    table.truncate()
    if filmes:
        table.insert_multiple(filmes)
    db.close()

def _obter_filme_catalogo(payload_filme):
    """
    Busca o filme no catálogo oficial para garantir que os IDs e metadados
    sejam sincronizados. Primeiro tenta por ID, depois por nome.
    """
    if not isinstance(payload_filme, dict):
        return None

    _bootstrap_catalogo_db()
    db = _get_catalogo_table()
    filmes = db.all()
    Filme = Query()
    filme_catalogo = None

    filme_id = payload_filme.get('id')
    if filme_id is not None:
        filme_catalogo = db.get(Filme.id == filme_id)

    if not filme_catalogo:
        nome_payload = payload_filme.get('nome')
        if isinstance(nome_payload, str):
            filme_catalogo = db.get(
                Filme.nome.test(
                    lambda valor: isinstance(valor, str) and valor.lower().strip() == nome_filme.lower().strip()
                )
            )

    return filme_catalogo.copy() if filme_catalogo else None


def adicionaFilme(payload):
    """
    Função principal: recebe o filme enviado pelo cliente, coloca na fila
    `filaFilmeAdicionado`, executa a validação e dispara a notificação final.

    payload esperado:
    {
        "usuario": "Débora",
        "filme": { ... },
        "status": "assistido" | "quero assistir"
    }
    """
    try:
        filaFilmeAdicionado.put(payload)
        validaAdicao()
        return disparaNotificacaoAdicao()
    except Exception as exc:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'sucesso': False,
                'mensagem': f'Erro ao adicionar filme: {exc}'
            }, ensure_ascii=False)
        }


def validaAdicao():
    """
    Consome `filaFilmeAdicionado`, valida status e atualiza `filmeUsuario.json`.
    Ao final, insere a confirmação/erro em `filaNotificaAdicao`.
    """
    try:
        mensagem = filaFilmeAdicionado.get(timeout=1)
    except Exception:
        return

    usuario = mensagem.get('usuario')
    filme = mensagem.get('filme')
    status = (mensagem.get('status') or '').lower().strip()

    if not usuario or not isinstance(filme, dict):
        filaNotificaAdicao.put({
            'sucesso': False,
            'mensagem': 'Campos obrigatórios ausentes (usuario/filme).'
        })
        return

    if status not in {'assistido', 'quero assistir'}:
        filaNotificaAdicao.put({
            'sucesso': False,
            'mensagem': 'Status inválido. Use "assistido" ou "quero assistir".'
        })
        return

    filme_catalogo = _obter_filme_catalogo(filme)
    if not filme_catalogo:
        filaNotificaAdicao.put({
            'sucesso': False,
            'mensagem': 'Filme não encontrado no catálogo oficial.'
        })
        return

    with _get_usuario_db() as db:
        usuarios_table = db.table('usuarios')
        Usuario = Query()
        usuario_doc = usuarios_table.get(
            Usuario.nome.test(lambda valor: isinstance(valor, str) and valor.lower() == usuario.lower())
        )

        registros_filmes = []
        if usuario_doc:
            registros_filmes = list(usuario_doc.get('filmes', []))

        nome_filme = filme_catalogo.get('nome')
        filme_id = filme_catalogo.get('id')

        existente = next(
            (
                f for f in registros_filmes
                if (filme_id is not None and f.get('id') == filme_id)
                or (nome_filme and f.get('nome', '').lower() == nome_filme.lower())
            ),
            None
        )

        registro_atualizado = {
            'id': filme_id,
            'nome': nome_filme,
            'descricao': filme_catalogo.get('descricao'),
            'status': status,
            'adicionado_em': datetime.utcnow().isoformat() + 'Z'
        }

        if existente:
            existente.update(registro_atualizado)
            msg = f'Filme "{nome_filme}" atualizado para "{status}".'
        else:
            registros_filmes.append(registro_atualizado)
            msg = f'Filme "{nome_filme}" adicionado com status "{status}".'

        usuario_id = None
        if usuario_doc:
            usuario_id = usuario_doc.doc_id
            usuarios_table.update({'filmes': registros_filmes}, doc_ids=[usuario_id])
        else:
            # Cria novo usuário e captura o doc_id retornado
            usuario_id = usuarios_table.insert({'nome': usuario, 'filmes': registros_filmes})

    filaNotificaAdicao.put({
        'sucesso': True,
        'mensagem': msg,
        'usuario': usuario,
        'usuario_id': usuario_id,
        'filme': registro_atualizado
    })


def disparaNotificacaoAdicao():
    """
    Consome `filaNotificaAdicao` e devolve resposta simulando um SNS.
    """
    try:
        notificacao = filaNotificaAdicao.get(timeout=1)
    except Exception:
        return {
            'statusCode': 504,
            'body': json.dumps({
                'sucesso': False,
                'mensagem': 'Nenhuma notificação disponível.'
            }, ensure_ascii=False)
        }

    status = 200 if notificacao.get('sucesso') else 400
    return {
        'statusCode': status,
        'body': json.dumps(notificacao, ensure_ascii=False, indent=2)
    }


# Teste local rápido
if __name__ == '__main__':
    exemplo = {
        'usuario': 'João',
        'filme': {'id': 2, 'descricao': 'Mesmo do catálogo'},
        'status': 'quero assistir'
    }
    print(adicionaFilme(exemplo)['body'])
