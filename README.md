# iFood Case Study

Este repositorio contem, todos os passos utilizados para o case do Ifood

---

## 📁 Estrutura do Projeto

 

ifood_cs/
│
├── main.py # Executa todo o pipeline
├── requirements.txt # Dependências do projeto
├── README.md # Documentação
│
├── Resultados/ # Resultados e saídas
│
├── code/
│ ├── script/
│ │ ├── load_data.py # Carregamento e preparação dos dados
│ │ ├── analysis.py # Scripts de análise
│ │ ├── case_item_1.py # Case 1
│ │ └── case_item_2.py # Case 2
│ │
│ └── utilities/
│ └── functions.py # Funções auxiliares
│
└── notebook/ # Notebooks utilizados para exploração e ajustes

---

## ▶️ Como Executar o Projeto

### **1. Criar ambiente virtual**

#### macOS / Linux
```bash
python3 -m venv venv
source venv/bin/activate
Windows
python -m venv venv
venv\Scripts\activate
2. Instalar dependências
pip install -r requirements.txt
3. Rodar o pipeline completo
python main.py
Livrary necessaria em requirements.txt
Notebooks: contém arquivos usados para exploracao de dados, bem como os parametros para serem alterados
Resultados: salva todos os outputs 



tterminal
