from flask import Flask, request, jsonify
from flask_cors import CORS
import sys
import os
import json

# Adiciona o diretório functions ao path para importar as funções
# Se o app.py estiver na raiz, descomente a linha abaixo
# sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'functions'))

from buscaFilme import buscaFilme
from adicionaFilme import adicionaFilme
from listarCatalogoUsuario import listarCatalogoUsuario
from cadastraFilmeDesejado import cadastraFilmeDesejado

app = Flask(__name__)
CORS(app)  # Permite requisições do Postman e outros clientes


@app.route('/')
def index():
    """Rota raiz com informações da API"""
    return jsonify({
        'mensagem': 'MovieFinder API',
        'versao': '1.0.0',
        'endpoints': {
            'buscar_filme': {
                'metodo': 'POST',
                'url': '/api/buscar-filme',
                'descricao': 'Busca um filme no catálogo',
                'body': {
                    'nome': 'string (nome do filme)'
                }
            },
            'adicionar_filme': {
                'metodo': 'POST',
                'url': '/api/adicionar-filme',
                'descricao': 'Adiciona um filme ao catálogo do usuário',
                'body': {
                    'usuario': 'string (nome do usuário)',
                    'filme': {
                        'id': 'integer (opcional, busca no catálogo se não fornecido)',
                        'nome': 'string (nome do filme)'
                    },
                    'status': 'string ("assistido" ou "quero assistir")'
                }
            },
            'listar_catalogo': {
                'metodo': 'GET',
                'url': '/api/listar-catalogo-usuario/<usuario_id>',
                'descricao': 'Lista o catálogo de filmes do usuário',
                'parametros': {
                    'usuario_id': 'integer (ID do usuário)'
                }
            },
            'cadastrar_filme_desejado': {
                'metodo': 'POST',
                'url': '/api/cadastrar-filme-desejado',
                'descricao': 'Cadastra um filme desejado para monitoramento (filme que ainda não está na plataforma)',
                'body': {
                    'usuario_id': 'integer (ID do usuário - preferencial)',
                    'usuario': 'string (nome do usuário - apenas se não houver nomes duplicados)',
                    'nome_filme': 'string (nome do filme a ser monitorado)'
                }
            }
        }
    }), 200


@app.route('/api/buscar-filme', methods=['POST'])
def api_buscar_filme():
    """
    Endpoint para buscar um filme no catálogo.
    
    Body esperado:
    {
        "nome": "O Enigma da Aurora"
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'sucesso': False,
                'mensagem': 'Body da requisição não fornecido'
            }), 400
        
        nome_filme = data.get('nome')
        
        if not nome_filme:
            return jsonify({
                'sucesso': False,
                'mensagem': 'Campo "nome" é obrigatório'
            }), 400
        
        # Chama a função de busca
        resultado = buscaFilme(nome_filme)
        
        # Converte o body (que é uma string JSON) de volta para dict
        body_dict = json.loads(resultado['body'])
        
        # Retorna com o status code apropriado
        return jsonify(body_dict), resultado['statusCode']
    
    except Exception as e:
        return jsonify({
            'sucesso': False,
            'mensagem': f'Erro ao processar requisição: {str(e)}'
        }), 500


@app.route('/api/adicionar-filme', methods=['POST'])
def api_adicionar_filme():
    """
    Endpoint para adicionar um filme ao catálogo do usuário.
    
    Body esperado:
    {
        "usuario": "Débora",
        "filme": {
            "id": 1,
            "nome": "O Enigma da Aurora"
        },
        "status": "assistido"
    }
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'sucesso': False,
                'mensagem': 'Body da requisição não fornecido'
            }), 400
        
        usuario = data.get('usuario')
        filme = data.get('filme')
        status = data.get('status')
        
        # Validações
        if not usuario:
            return jsonify({
                'sucesso': False,
                'mensagem': 'Campo "usuario" é obrigatório'
            }), 400
        
        if not filme:
            return jsonify({
                'sucesso': False,
                'mensagem': 'Campo "filme" é obrigatório'
            }), 400
        
        if not status:
            return jsonify({
                'sucesso': False,
                'mensagem': 'Campo "status" é obrigatório'
            }), 400
        
        if status.lower() not in ['assistido', 'quero assistir']:
            return jsonify({
                'sucesso': False,
                'mensagem': 'Status deve ser "assistido" ou "quero assistir"'
            }), 400
        
        # Prepara o payload
        payload = {
            'usuario': usuario,
            'filme': filme,
            'status': status
        }
        
        # Chama a função de adicionar filme
        resultado = adicionaFilme(payload)
        
        # Converte o body (que é uma string JSON) de volta para dict
        body_dict = json.loads(resultado['body'])
        
        # Retorna com o status code apropriado
        return jsonify(body_dict), resultado['statusCode']
    
    except Exception as e:
        return jsonify({
            'sucesso': False,
            'mensagem': f'Erro ao processar requisição: {str(e)}'
        }), 500


@app.route('/api/listar-catalogo-usuario/<int:usuario_id>', methods=['GET'])
def api_listar_catalogo_usuario(usuario_id):
    """
    Endpoint para listar o catálogo de filmes do usuário.
    
    Parâmetros:
    - usuario_id: ID do usuário (integer)
    """
    try:
        # Chama a função de listar catálogo
        resultado = listarCatalogoUsuario(usuario_id)
        
        # Converte o body (que é uma string JSON) de volta para dict
        body_dict = json.loads(resultado['body'])
        
        # Retorna com o status code apropriado
        return jsonify(body_dict), resultado['statusCode']
    
    except ValueError:
        return jsonify({
            'sucesso': False,
            'mensagem': 'ID do usuário deve ser um número inteiro'
        }), 400
    
    except Exception as e:
        return jsonify({
            'sucesso': False,
            'mensagem': f'Erro ao processar requisição: {str(e)}'
        }), 500


@app.route('/api/cadastrar-filme-desejado', methods=['POST'])
def api_cadastrar_filme_desejado():
    """
    Endpoint para cadastrar um filme desejado para monitoramento.
    
    Body esperado (preferencial):
    {
        "usuario_id": 1,
        "nome_filme": "Matrix 5"
    }
    ou (apenas se não houver nomes duplicados):
    {
        "usuario": "Débora",
        "nome_filme": "Matrix 5"
    }
    
    Nota: Sempre use usuario_id quando possível para evitar ambiguidade.
    """
    try:
        data = request.get_json()
        
        if not data:
            return jsonify({
                'sucesso': False,
                'mensagem': 'Body da requisição não fornecido'
            }), 400
        
        usuario = data.get('usuario')
        usuario_id = data.get('usuario_id')
        nome_filme = data.get('nome_filme')
        
        # Validações
        if not nome_filme:
            return jsonify({
                'sucesso': False,
                'mensagem': 'Campo "nome_filme" é obrigatório'
            }), 400
        
        if not usuario_id and not usuario:
            return jsonify({
                'sucesso': False,
                'mensagem': 'Campo "usuario_id" é obrigatório (ou "usuario" se não houver nomes duplicados)'
            }), 400
        
        # Prepara o payload
        payload = {
            'nome_filme': nome_filme
        }
        
        if usuario_id:
            payload['usuario_id'] = usuario_id
        if usuario:
            payload['usuario'] = usuario
        
        # Chama a função de cadastrar filme desejado
        resultado = cadastraFilmeDesejado(payload)
        
        # Converte o body (que é uma string JSON) de volta para dict
        body_dict = json.loads(resultado['body'])
        
        # Retorna com o status code apropriado
        return jsonify(body_dict), resultado['statusCode']
    
    except Exception as e:
        return jsonify({
            'sucesso': False,
            'mensagem': f'Erro ao processar requisição: {str(e)}'
        }), 500


@app.errorhandler(404)
def not_found(error):
    """Handler para rotas não encontradas"""
    return jsonify({
        'sucesso': False,
        'mensagem': 'Rota não encontrada'
    }), 404


@app.errorhandler(500)
def internal_error(error):
    """Handler para erros internos"""
    return jsonify({
        'sucesso': False,
        'mensagem': 'Erro interno do servidor'
    }), 500


if __name__ == '__main__':
    print("=" * 50)
    print("MovieFinder API - Servidor Flask")
    print("=" * 50)
    print("\nEndpoints disponíveis:")
    print("  POST   /api/buscar-filme")
    print("  POST   /api/adicionar-filme")
    print("  GET    /api/listar-catalogo-usuario/<usuario_id>")
    print("  POST   /api/cadastrar-filme-desejado")
    print("\nServidor rodando em: http://localhost:5000")
    print("Documentação da API: http://localhost:5000/")
    print("=" * 50)
    print("\nPressione Ctrl+C para parar o servidor\n")
    
    app.run(debug=True, host='0.0.0.0', port=5000)

