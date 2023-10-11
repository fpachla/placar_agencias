# placar_agencias
Olá, esse aqui é meu repo de exemplo

```python
print('hello world!')

```
Lembre de criar um venv no seu repositório, pra que fique fácil 
de reproduzir ele em outra máquina.
> ai adiciona a pasta do venv no arquivo **".gitignore"** que deve ficar na raiz do repositório

```git
# ignorar pasta do venv 
.venv

# ignorar caches de cpython
**\__pycache__\

```

Vai instalando as bibliotecas conforme o necessário e sempre lembre 
rodar o pip freeze, para deixar as dependencias 'up to date'

```
pip freeze > requirements.txt
```

Depois quando quiser instalar numa nova máquina é só rodar

```
pip install -r requirements.txt
```