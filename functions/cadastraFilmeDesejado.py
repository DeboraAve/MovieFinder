import json
import os
from datetime import datetime
from queue import Queue
from tinydb import TinyDB, Query

# Filas para simular o pipeline (SQS/SNS)
filaFilmeDesejado = Queue()  # Fila que recebe o filme desejado
filaRetornoDesejados = Queue()  # Fila que recebe os retornos para notificação

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
USUARIO_JSON = os.path.join(BASE_DIR, 'data', 'filmeUsuario.json')
CATALOGO_JSON = os.path.join(BASE_DIR, 'data', 'filmes.json')
DESEJADOS_JSON = os.path.join(BASE_DIR, 'data', 'filmesDesejados.json')


def _get_usuario_db():
    """
    Retorna uma instância do TinyDB para o arquivo de filmes do usuário.
    """
    db = TinyDB(USUARIO_JSON, ensure_ascii=False, indent=2, encoding='utf-8' )
    db.table('usuarios')
    return db


def _get_desejados_db():
    db = TinyDB(DESEJADOS_JSON, ensure_ascii=False, indent=2, encoding='utf-8')
    return db.table("FilmesDesejados")

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


def _obter_usuario_id(usuario_nome):
    """
    Obtém o ID do usuário (doc_id) pelo nome.
    Retorna None se não encontrar ou se houver múltiplos usuários com o mesmo nome.
    """
    with _get_usuario_db() as db:
        usuarios_table = db.table('usuarios')
        Usuario = Query()
        # Busca todos os usuários com esse nome (pode haver duplicatas)
        usuarios = usuarios_table.search(
            Usuario.nome.test(lambda valor: isinstance(valor, str) and valor.lower() == usuario_nome.lower())
        )
        # Se houver múltiplos, retorna None para forçar uso de ID
        if len(usuarios) > 1:
            return None
        return usuarios[0].doc_id if usuarios else None


def _validar_usuario_id(usuario_id):
    """
    Valida se o usuario_id existe no banco de dados.
    Retorna True se existe, False caso contrário.
    """
    if usuario_id is None:
        return False
    
    with _get_usuario_db() as db:
        usuarios_table = db.table('usuarios')
        usuario_doc = usuarios_table.get(doc_id=usuario_id)
        return usuario_doc is not None


def _buscar_filme_catalogo(nome_filme):
    """
    Busca o filme no catálogo principal (filmes.json).
    Retorna o filme se encontrado, None caso contrário.
    """
    _bootstrap_catalogo_db()
    db = _get_catalogo_table()
    filmes = db.all()
    Filme = Query()
    filme = db.get(
        Filme.nome.test(
                lambda valor: isinstance(valor, str) and valor.lower().strip() == nome_filme.lower().strip()
            )
    )
    return filme.copy() if filme else None


def _buscar_filme_desejado(nome_filme):
    """
    Busca o filme na lista de desejados (filmesDesejados.json).
    Retorna o documento se encontrado, None caso contrário.
    """
    if not os.path.exists(DESEJADOS_JSON) or os.path.getsize(DESEJADOS_JSON) == 0:
        return None
    
    db = _get_desejados_db()
    FilmeDesejado = Query()
    filme = db.get(
        FilmeDesejado.nome.test(lambda v: isinstance(v, str) and v.lower().strip() == nome_filme.lower().strip())
    )
    return filme.copy() if filme else None



def cadastraFilmeDesejado(payload):
    """
    Função principal: recebe o filme desejado pelo usuário, coloca na fila
    `filaFilmeDesejado`, executa a validação e dispara a notificação final.

    payload esperado:
    {
        "usuario_id": 1,  # Preferencial - ID único do usuário
        "nome_filme": "Nome do Filme"
    }
    ou (se não houver ambiguidade de nomes):
    {
        "usuario": "Débora",  # Busca o ID pelo nome (não recomendado se houver nomes duplicados)
        "nome_filme": "Nome do Filme"
    }
    
    Nota: Sempre usa usuario_id internamente para evitar ambiguidade.
    """
    try:
        filaFilmeDesejado.put(payload)
        validaFilmeDesejado()
        return dispararNotificacaoDesejados()
    except Exception as exc:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'sucesso': False,
                'mensagem': f'Erro ao cadastrar filme desejado: {exc}'
            }, ensure_ascii=False)
        }


def validaFilmeDesejado():
    """
    Consome `filaFilmeDesejado`, valida se o filme existe no sistema e
    atualiza `filmesDesejados.json` se necessário.
    Ao final, insere o resultado em `filaRetornoDesejados`.
    """
    try:
        mensagem = filaFilmeDesejado.get(timeout=1)
    except Exception:
        return

    usuario_nome = mensagem.get('usuario')
    usuario_id = mensagem.get('usuario_id')
    nome_filme = mensagem.get('nome_filme')

    # Validações básicas
    if not nome_filme:
        filaRetornoDesejados.put({
            'sucesso': False,
            'mensagem': 'Campo "nome_filme" é obrigatório.'
        })
        return

    # Obtém o ID do usuário se não foi fornecido diretamente
    if not usuario_id:
        if usuario_nome:
            usuario_id = _obter_usuario_id(usuario_nome)
            if not usuario_id:
                filaRetornoDesejados.put({
                    'sucesso': False,
                    'mensagem': f'Usuário "{usuario_nome}" não encontrado no sistema ou há múltiplos usuários com esse nome. Use "usuario_id" para especificar.'
                })
                return
        else:
            filaRetornoDesejados.put({
                'sucesso': False,
                'mensagem': 'Campo "usuario_id" é obrigatório (ou "usuario" se não houver ambiguidade).'
            })
            return

    # Valida que o usuario_id existe no banco de dados
    if not _validar_usuario_id(usuario_id):
        filaRetornoDesejados.put({
            'sucesso': False,
            'mensagem': f'Usuário com ID "{usuario_id}" não encontrado no sistema.'
        })
        return

    # Busca o filme no catálogo principal
    filme_catalogo = _buscar_filme_catalogo(nome_filme)

    # Caso 1: Filme já existe no catálogo principal
    if filme_catalogo:
        filaRetornoDesejados.put({
            'sucesso': True,
            'mensagem': f'Filme "{nome_filme}" já está disponível na plataforma!',
            'tipo': 'filme_disponivel',
            'usuario_id': usuario_id,
            'filme': filme_catalogo
        })
        return

    # Busca o filme na lista de desejados
    filme_desejado = _buscar_filme_desejado(nome_filme)

    # Caso 2: Filme já está sendo monitorado
    if filme_desejado:
        usuarios_interessados = filme_desejado.get('usuarios_interessados', [])
        
        # Adiciona o novo usuário se ainda não estiver na lista
        if usuario_id not in usuarios_interessados:
            usuarios_interessados.append(usuario_id)
            
            # Atualiza o registro
            db = _get_desejados_db()
            FilmeDesejado = Query()
            db.update(
                {'usuarios_interessados': usuarios_interessados},
                FilmeDesejado.nome.test(lambda v: isinstance(v, str) and v.lower().strip() == nome_filme.lower().strip())
            )


        filaRetornoDesejados.put({
            'sucesso': True,
            'mensagem': f'Filme "{nome_filme}" já está sendo monitorado. Você será notificado quando estiver disponível!',
            'tipo': 'ja_monitorado',
            'usuario_id': usuario_id,
            'filme_desejado': {
                'nome': nome_filme,
                'cadastrado_em': filme_desejado.get('cadastrado_em'),
                'total_interessados': len(usuarios_interessados)
            }
        })
        return

    # Caso 3: Filme não existe em nenhum lugar - cadastra novo
    novo_filme_desejado = {
        'nome': nome_filme,
        'usuarios_interessados': [usuario_id],
        'cadastrado_em': datetime.utcnow().isoformat() + 'Z'
    }

    db = _get_desejados_db()
    db.insert(novo_filme_desejado)


    filaRetornoDesejados.put({
        'sucesso': True,
        'mensagem': f'Filme "{nome_filme}" cadastrado para monitoramento. Você será notificado quando estiver disponível!',
        'tipo': 'novo_cadastro',
        'usuario_id': usuario_id,
        'filme_desejado': novo_filme_desejado
    })


def dispararNotificacaoDesejados():
    """
    Consome `filaRetornoDesejados` e devolve resposta simulando um SNS.
    """
    try:
        notificacao = filaRetornoDesejados.get(timeout=1)
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
    # Teste 1: Cadastrar novo filme desejado
    print("=== Teste 1: Cadastrar novo filme desejado ===")
    exemplo1 = {
        'usuario': 'João',
        'nome_filme': 'Matrix 5'
    }
    resultado1 = cadastraFilmeDesejado(exemplo1)
    print(resultado1['body'])
    print()

    # Teste 2: Tentar cadastrar filme que já está no catálogo
    print("=== Teste 2: Filme já no catálogo ===")
    exemplo2 = {
        'usuario': 'Débora',
        'nome_filme': 'O Enigma da Aurora'
    }
    resultado2 = cadastraFilmeDesejado(exemplo2)
    print(resultado2['body'])
    print()


