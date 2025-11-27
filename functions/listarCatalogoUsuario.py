import json
import os
from tinydb import TinyDB, Query

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
USUARIO_JSON = os.path.join(BASE_DIR, 'data', 'filmeUsuario.json')
CATALOGO_JSON = os.path.join(BASE_DIR, 'data', 'filmes.json')


def _get_usuario_db():
    """
    Retorna uma instância do TinyDB para o arquivo de filmes do usuário.
    """
    db = TinyDB(USUARIO_JSON, ensure_ascii=False, indent=2)
    db.table('usuarios')
    return db


def _get_catalogo_db():
    """
    Retorna uma instância do TinyDB para o catálogo de filmes.
    """
    return TinyDB(CATALOGO_JSON, ensure_ascii=False, indent=2)


def listarCatalogoUsuario(usuario_id):
    """
    Função principal que consulta no banco de dados e lista os filmes do usuário,
    separados por status (assistido ou quero assistir).
    
    Args:
        usuario_id: Integer ou String com o ID do usuário (doc_id do TinyDB)
    
    Returns:
        Dicionário com statusCode e body contendo a lista de filmes separados por status
    """
    
    try:
        # Converte para inteiro se for string
        if isinstance(usuario_id, str):
            try:
                usuario_id = int(usuario_id)
            except ValueError:
                return {
                    'statusCode': 400,
                    'body': json.dumps({
                        'sucesso': False,
                        'mensagem': 'ID do usuário inválido. Deve ser um número.',
                        'dados': None
                    }, ensure_ascii=False)
                }
        
        if usuario_id is None:
            return {
                'statusCode': 400,
                'body': json.dumps({
                    'sucesso': False,
                    'mensagem': 'ID do usuário não fornecido',
                    'dados': None
                }, ensure_ascii=False)
            }
        
        # Consulta o banco de dados do usuário
        with _get_usuario_db() as db:
            usuarios_table = db.table('usuarios')
            
            # Busca o usuário pelo doc_id (ID único do TinyDB)
            usuario_doc = usuarios_table.get(doc_id=usuario_id)
            
            if not usuario_doc:
                return {
                    'statusCode': 404,
                    'body': json.dumps({
                        'sucesso': False,
                        'mensagem': f'Usuário com ID "{usuario_id}" não encontrado',
                        'dados': {
                            'usuario_id': usuario_id,
                            'assistidos': [],
                            'quero_assistir': [],
                            'total': 0
                        }
                    }, ensure_ascii=False)
                }
            
            filmes_usuario = usuario_doc.get('filmes', [])
            
            # Separa filmes por status
            filmes_assistidos = []
            filmes_quero_assistir = []
            
            # Consulta o catálogo para enriquecer os dados dos filmes
            with _get_catalogo_db() as catalogo_db:
                Filme = Query()
                
                for filme in filmes_usuario:
                    filme_id = filme.get('id')
                    status = filme.get('status', '').lower().strip()
                    
                    # Busca informações completas no catálogo
                    filme_catalogo = None
                    if filme_id is not None:
                        filme_catalogo = catalogo_db.get(Filme.id == filme_id)
                    
                    # Prepara o filme com informações do catálogo
                    filme_completo = {
                        'id': filme_id,
                        'nome': filme.get('nome'),
                        'descricao': filme.get('descricao'),
                        'status': status,
                        'adicionado_em': filme.get('adicionado_em')
                    }
                    
                    # Adiciona detalhes do catálogo se encontrado
                    if filme_catalogo:
                        filme_completo['detalhes'] = filme_catalogo.get('detalhes', {})
                        filme_completo['streamings'] = filme_catalogo.get('streamings', [])
                    
                    # Separa por status
                    if status == 'assistido':
                        filmes_assistidos.append(filme_completo)
                    elif status == 'quero assistir':
                        filmes_quero_assistir.append(filme_completo)
            
            # Prepara resposta
            resultado = {
                'sucesso': True,
                'mensagem': f'Catálogo do usuário listado com sucesso',
                'dados': {
                    'usuario_id': usuario_id,
                    'usuario': usuario_doc.get('nome'),
                    'assistidos': filmes_assistidos,
                    'quero_assistir': filmes_quero_assistir,
                    'total': len(filmes_usuario),
                    'total_assistidos': len(filmes_assistidos),
                    'total_quero_assistir': len(filmes_quero_assistir)
                }
            }
            
            return {
                'statusCode': 200,
                'body': json.dumps(resultado, ensure_ascii=False, indent=2)
            }
    
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps({
                'sucesso': False,
                'mensagem': f'Erro ao listar catálogo do usuário: {str(e)}',
                'dados': None
            }, ensure_ascii=False)
        }


# Exemplo de uso para testes locais
if __name__ == '__main__':
    # Teste 1: Listar catálogo de um usuário existente (usando doc_id = 1)
    print("=== Teste 1: Listar catálogo do usuário (ID = 1) ===")
    resultado1 = listarCatalogoUsuario(1)
    print(resultado1['body'])
    print()
    
    # Teste 2: Usuário não encontrado
    print("=== Teste 2: Usuário não encontrado (ID = 999) ===")
    resultado2 = listarCatalogoUsuario(999)
    print(resultado2['body'])
    print()
    
    # Teste 3: ID inválido
    print("=== Teste 3: ID inválido ===")
    resultado3 = listarCatalogoUsuario("abc")
    print(resultado3['body'])
    print()
    
    # Teste 4: Sem ID
    print("=== Teste 4: Sem ID ===")
    resultado4 = listarCatalogoUsuario(None)
    print(resultado4['body'])

