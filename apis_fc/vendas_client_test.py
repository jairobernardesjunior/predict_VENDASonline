import requests

url = ' http://127.0.0.1:5000/vendas/'

#url = ' http://localhost:5000/vendas/'

response = requests.get(url)
print(response.text)