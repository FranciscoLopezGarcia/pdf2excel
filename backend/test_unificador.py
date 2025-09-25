from extractors.unificador import unir_consolidados

files = [
    r"C:\Users\FranciscoLópezGarcía\Downloads\resultado (1)\consolidado.xlsx",
    r"C:\Users\FranciscoLópezGarcía\Downloads\resultado (2)\consolidado.xlsx",
    # seguí agregando los que tengas...
]

unir_consolidados(files, r"C:\Users\FranciscoLópezGarcía\Downloads\consolidado_anual.xlsx")
