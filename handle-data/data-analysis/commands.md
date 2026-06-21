# Comandos para executar contagion.py

## Executar todos os 3 modelos de análise
python contagion.py --nodes nodes.json --edges edges.json --model all

## Executar individualmente cada modelo de análise
python contagion.py --nodes nodes.json --edges edges.json --model sis --beta 0.4 --gamma 0.1 --steps 50
python contagion.py --nodes nodes.json --edges edges.json --model granovetter --theta 0.25 --sigma 0.15
python contagion.py --nodes nodes.json --edges edges.json --model betweenness --top_k 20

## Executar, salvar gráficos e exportar JSON
python contagion.py --nodes nodes.json --edges edges.json --model all --save_plots --save_json


# Comandos para executar permutation_test.py

python permutation_test.py --nodes nodes.json --edges edges.json --n_perm 100 --mode weight --save_plot perm_weight.png --save_json perm_weight.json