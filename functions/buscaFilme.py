import json
import os
from queue import Queue
from difflib import SequenceMatcher
from tinydb import TinyDB, Query

# Filas globais para simular o comportamento de filas de mensagens
filaBuscaFilme = Queue()  # Fila que recebe o nome do filme a ser buscado
filaEncontrado = Queue()  # Fila que recebe os filmes encontrados/validados

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
CATALOGO_DB_PATH = os.path.join(BASE_DIR, 'data', 'filmes.json')


def _bootstrap_catalogo_db():
    """
    Garante que o arquivo de catálogo esteja no formato esperado pelo TinyDB.
    Se estiver no formato antigo (lista simples ou dict com chave 'filmes'),
    converte automaticamente para o formato interno do TinyDB.
    """
    if not os.path.exists(CATALOGO_DB_PATH):
        TinyDB(CATALOGO_DB_PATH, ensure_ascii=False, indent=2).close()
        return

    try:
        with open(CATALOGO_DB_PATH, 'r', encoding='utf-8') as f:
            if os.path.getsize(CATALOGO_DB_PATH) == 0:
                raise json.JSONDecodeError('empty', '', 0)
            raw_data = json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        TinyDB(CATALOGO_DB_PATH, ensure_ascii=False, indent=2).close()
        return

    if isinstance(raw_data, dict) and '_default' in raw_data:
        return

    filmes = []
    if isinstance(raw_data, dict) and isinstance(raw_data.get('filmes'), list):
        filmes = raw_data['filmes']
    elif isinstance(raw_data, list):
        filmes = raw_data

    db = TinyDB(CATALOGO_DB_PATH, ensure_ascii=False, indent=2)
    db.truncate()
    if filmes:
        db.insert_multiple(filmes)
    db.close()

def similaridade(a, b):
    """
    Calcula a similaridade entre duas strings.
    Retorna um valor entre 0 e 1.
    """
    return SequenceMatcher(None, a.lower(), b.lower()).ratio()

def buscaFilme(nome_filme):
    """
    Função principal que imita o comportamento de uma Lambda para buscar um filme.
    Recebe o nome do filme e insere na filaBuscaFilme.
    Pode executar validaFilme() e retornaFilme() internamente.
    
    Args:
        nome_filme: String com o nome do filme a ser buscado
                   Exemplo: "O Enigma da Aurora"
    
    Returns:
        Resultado formatado da busca (se executar validaFilme e retornaFilme internamente)
        ou None (apenas insere na fila)
    """
    
    try:
        # Se nome_filme for um dicionário, extrai o nome
        if isinstance(nome_filme, dict):
            nome = nome_filme.get('nome') or nome_filme.get('titulo') or nome_filme.get('title')
        else:
            nome = nome_filme
        
        if not nome:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'erro': True,
                    'mensagem': 'Nome do filme não fornecido',
                    'dados': None
                }, ensure_ascii=False)
            }
        
        # Insere o nome do filme na filaBuscaFilme
        filaBuscaFilme.put(nome)
        
        # Executa validaFilme para processar a busca
        validaFilme()
        
        # Executa retornaFilme para retornar o resultado
        resultado = retornaFilme()
        
        return resultado
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'erro': True,
                'mensagem': f'Erro ao buscar filme: {str(e)}',
                'dados': None
            }, ensure_ascii=False)
        }

def validaFilme():
    """
    Função que consome o filme inserido na filaBuscaFilme.
    Valida se encontrou um match exato ou similares no banco de dados.
    Retorna o filme se deu match e/ou possíveis similares se não deu match exato.
    Calcula a similaridade e insere na filaEncontrado.
    
    Returns:
        None (resultado é inserido na filaEncontrado)
    """
    
    try:
        nome_filme = filaBuscaFilme.get(timeout=1)

        _bootstrap_catalogo_db()
        with TinyDB(CATALOGO_DB_PATH, ensure_ascii=False, indent=2) as db:
            filmes = db.all()
            Filme = Query()
            match_doc = db.get(
                Filme.nome.test(lambda valor: isinstance(valor, str) and valor.lower().strip() == nome_filme.lower().strip())
            )

        if not filmes:
            filaEncontrado.put({
                'erro': True,
                'mensagem': 'Catálogo de filmes vazio',
                'dados': None,
                'match_exato': False,
                'similares': []
            })
            return

        nome_busca = nome_filme.lower().strip()
        filme_match_exato = match_doc.copy() if match_doc else None
        filmes_similares = []

        if not filme_match_exato:
            for filme in filmes:
                nome_filme_db = (filme.get('nome') or '').lower().strip()
                if not nome_filme_db:
                    continue

                sim = similaridade(nome_busca, nome_filme_db)
                if sim > 0.5:
                    filme_similar = filme.copy()
                    filme_similar['similaridade'] = round(sim, 2)
                    filmes_similares.append(filme_similar)

            filmes_similares.sort(key=lambda x: x.get('similaridade', 0), reverse=True)

        if filme_match_exato:
            mensagem = {
                'erro': False,
                'mensagem': 'Match exato encontrado',
                'dados': filme_match_exato,
                'match_exato': True,
                'similares': []
            }
        else:
            mensagem = {
                'erro': False,
                'mensagem': f'Match exato não encontrado. {len(filmes_similares)} similar(es) encontrado(s)' if filmes_similares else 'Nenhum filme encontrado',
                'dados': None,
                'match_exato': False,
                'similares': filmes_similares[:5]
            }

        filaEncontrado.put(mensagem)

    except Exception as e:
        filaEncontrado.put({
            'erro': True,
            'mensagem': f'Erro ao validar filme: {str(e)}',
            'dados': None,
            'match_exato': False,
            'similares': []
        })

def retornaFilme():
    """
    Função que consome o filme encontrado da filaEncontrado.
    Retorna para o usuário os dados encontrados formatados.
    
    Returns:
        Dicionário com statusCode e body contendo o resultado formatado
    """
    
    try:
        # Consome mensagem da filaEncontrado
        mensagem = filaEncontrado.get(timeout=1)
        
        # Verifica se houve erro
        if mensagem.get('erro'):
            return {
                'statusCode': 404 if 'não encontrado' in mensagem.get('mensagem', '') else 500,
                'body': json.dumps({
                    'sucesso': False,
                    'erro': mensagem.get('mensagem', 'Erro desconhecido'),
                    'dados': None,
                    'match_exato': False,
                    'similares': []
                }, ensure_ascii=False)
            }
        
        match_exato = mensagem.get('match_exato', False)
        dados = mensagem.get('dados')
        similares = mensagem.get('similares', [])
        
        # Formata o resultado para o usuário
        resultado_formatado = {
            'sucesso': True,
            'mensagem': mensagem.get('mensagem', ''),
            'match_exato': match_exato,
            'dados': None,
            'similares': []
        }
        
        # Se houve match exato
        if match_exato and dados:
            resultado_formatado['dados'] = {
                'id': dados.get('id'),
                'nome': dados.get('nome'),
                'descricao': dados.get('descricao'),
                'detalhes': dados.get('detalhes', {}),
                'streamings': dados.get('streamings', [])
            }
        
        # Processa similares (indicações)
        for similar in similares:
            similar_formatado = {
                'id': similar.get('id'),
                'nome': similar.get('nome'),
                'descricao': similar.get('descricao'),
                'detalhes': similar.get('detalhes', {}),
                'streamings': similar.get('streamings', []),
                'similaridade': similar.get('similaridade', 0)
            }
            resultado_formatado['similares'].append(similar_formatado)
        
        status_code = 200 if match_exato or similares else 404
        
        return {
            'statusCode': status_code,
            'body': json.dumps(resultado_formatado, ensure_ascii=False, indent=2)
        }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'sucesso': False,
                'erro': f'Erro ao retornar filme: {str(e)}',
                'dados': None,
                'match_exato': False,
                'similares': []
            }, ensure_ascii=False)
        }


# Exemplo de uso para testes locais
if __name__ == '__main__':
    # Teste 1: Busca com match exato
    print("=== Teste 1: Busca com match exato ===")
    resultado1 = buscaFilme("O Enigma da Aurora")
    print(resultado1['body'])
    print()
    
    # Teste 2: Busca sem match exato (deve retornar similares)
    print("=== Teste 2: Busca sem match exato ===")
    resultado2 = buscaFilme("Enigma Aurora")
    print(resultado2['body'])
    print()
    
    # Teste 3: Busca que não encontra nada
    print("=== Teste 3: Busca sem resultados ===")
    resultado3 = buscaFilme("Filme Inexistente XYZ")
    print(resultado3['body'])
