
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'code'))

def main():
   
    
    # Pipeline
    from script.load_data import main as pipeline_main
    pipeline_main()
    
    # Análise
    from script.analisys import executar_analise_completa as analise_main
    analise_main()
    
    # Case 1 - Importa e executa
    import script.case_item_1
 
    
    # Case 2 - Importa e executa  
    import script.case_item_2

if __name__ == "__main__":
    main()