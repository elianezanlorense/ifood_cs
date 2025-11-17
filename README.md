# 📊 iFood Case Study

Este repositório contém o *case study* desenvolvido para o processo seletivo do iFood.  
Aqui você encontra todo o pipeline de análise, preparação de dados, scripts, notebooks e resultados finais.

---

## 📁 Estrutura do Projeto

 

#This readme is an overview of the case study for Ifood
ifood_cs/
│── main.py              # run the whole pipeline
│── requirements.txt
│── README.md
│── Resultados/          # results
│
└── code/
    ├── script/
    │   ├── load_data.py        # load and prep
    │   ├── analisys.py         # analysis; migration
    │   ├── case_item_1.py      # case 1 
    │   └── case_item_2.py      # case 2 
    │
    ├── utilities/
    │   └── functions.py        # functions
    │
    └── notebook/               # notebooks used to biuld the scripts/ to see more details of the data set, and also change parameters

to run: 
python3 -m venv venv
source venv/bin/activate      # macOS / Linux
venv\Scripts\activate         # Windows
install:
pip install -r requirements.txt
to run: python main.py - bash/terminal
