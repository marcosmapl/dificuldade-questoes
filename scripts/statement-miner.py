"""
	Esse script recupera os enunciados de questões do dataset Codebench, submete esses enunciados
	para extração de métricas de inteligibilidade textual no sistema online Coh-Metrix-Port 3.0.

	Os enunciados são extraídos de um arquivo CSV do dataset Condebench.
	Como os enunciados estão armazenados dentro do arquivo no formato HTML é necessário remover tags e deixar somente o
	texto.
	Para a remoção de tags e seleção somente do texto dos enunciados faz-se uso da biblioteca BeautifulSoup.
	Em seguida, o enunciado é "limpo" por meio de algumas funções que utilizam Expressões Regulares (regex).
	Essa funções são aplicadas com o objetivo de:
		- Remover caracteres especiais que estão relacionados com a inteligibilidade do enunciado, com por exemplo "\" e "@";
		- Substituição de alguns caracteres por sua forma por extenso, como por exemplo ">" por "maior que";
		- Remover algumas urls que por ventura estão inseridas no enunciado das questões;
		- Substituir valores numéricos no enunciado pela expressão "número";
		- Substituir valores monetários em reais no enunciado pela expressão "BRL";
		- Substituir fórmulas em latex no enunciado pela expressão "latex";
	Com o enunciado limpo é feita a submissão no sistema online Coh-Metrix-Port 3.0.
	Esse sistema está disponível no endereço: http://fw.nilc.icmc.usp.br:23380/cohmetrixport
	Os sistema só permite a submissão de 01 enunciado por vez, e após feita a submissão é gerada uma tabela com
	as métricas extraídas do enunciado e também um botão para download das métricas num arquivo CSV.
	Devido a limitação de submissão, automatizamos o processo por meio da biblioteca Selenium.
	A biblioteca Selenium cria uma instância controlado de um navegador web que esteja instalado no
	ambiente de execução deste script. Para criar uma instância do navegar ela faz uso de um driver.
	O arquivo de métricas é salvo num pasta definida nas configurações desse script e logo após ele é renomeado
	recebendo como nome o código da questão (id de identificação).
	Após o download a submissão de todos os enunciados e download de seus arquivos de métricas este script
	faz a unificação de todas os arquivos de métricas num único arquivo CSV.

	@author marcos.lima@icomp.ufam.edu.br
	@since 04, Fev 2021.
	@version 1.2.2
"""
import os
from typing import List

import pandas as pd
import re
import time
from bs4 import BeautifulSoup
from bs4.element import Comment
from selenium import webdriver
from selenium.webdriver.chrome.options import Options


# Dicionário com os caracteres especiais a serem substituídos/removidos do enunciado.
# Este dicionário usa como chave o código unicode do caractere.
to_replace = {
	b'\\u0131': 'i',  # dotless i
	b'\\u0155': 'r',  # letter r acute accent
	b'\\u03b1': ' alfa ',
	b'@': ' arroba ',
	b'#': ' cerquilha ',
	b'\\u2206': ' delta ',
	b'$': ' dólar ',
	b'\\xb0': ' graus ',
	b'\\u221e': ' infinito ',
	b'\\u0142': ' libra ',
	b'\\u2113': ' litro ',
	b'\\u03c9': ' ohm ',
	b'\\xb2': ' ao quadrado ',
	b'\\xb3': ' ao cubo ',
	b'\\u221a': ' raiz quadrada ',
	b'+': ' mais ',
	b'\\2212': ' menos ',
	b'\\xd7': ' multiplicação ',
	b'=': ' igual ',
	b'\\u2264': ' menor ou igual ',
	b'\\u2265': ' maior ou igual que ',
	# b'*': ' ',
	# b'%': ' por cento ',
	# b'?': ' question-mark ',
	b'\\u22c5': ' ',  # vectorial product symbol
	b'\\u2219': ' ',  # scalar product symbol
	b'\\xb7': ' ',  # bold middot
	b'>': ' maior que ',
	b'\\u03c0': ' pi ',
	b'\\xf7': ' divisão ',
	b'<': ' menor que ',
	b'/': ' ',  # forward slash
	b'\\\\': ' ',  # backward slash
	b'|': ' ',  # vertical bar
	b'\\u2192': ' ',  # rightward set
	b'\\u2013': ' ',  # dash symbol
	b"'": ' ',  # apostrophe
	b'\\u2019': ' ',  # apostrophe
	b'\\u2018': ' ',  # single quote
	b'"': ' ',  # double quote
	b'\\u201c': ' ',  # open citation quote
	b'\\u201d': ' ',  # close citation quote
	b'\\xb4': ' ',  # acude accent
	b'`': ' ',  # acento grave
	b'^': ' ',  # cincumflex
	b'&': ' ',  # ampersand
	b';': '. ',
	b'_': ' ',  # underscores
	b'\\u2026': ' '  # 3 fullstop (horizontal ellipsis)
	# b'-': ' hífen ',
	# b':': ' colon ',
	# b'(': ' open-parenthesis ',
	# b')': ' close-parenthesis ',
	# b'[': ' abre colchetes ',
	# b']': ' fecha colchetes ',
	# b'{': ' abre chaves ',
	# b'}': ' fecha chaves ',
	# b'\\xaa': ' ordinal feminino ',
	# b'\\xba': ' ordinal masculino ',
}

# dicionário de configurações do script
# cwd: diretório atual de execução do script
# url_pattern: regex para urls
# latex_patterns: lista com regex para expressões em latex
# numerical_patterns: lista com regex para valores numéricas (inteiros, ponto-flutuantes e porcentagens)
# currency_patterns: lista com regex para valores monetários em reais (BRL).
# driver_file: caminho do driver para o navegador web. o driver deve ser baixado de acordo com a versão do seu navegador
# coh_metrix_port_url: url para o sistema Coh-Metrix-Port 3.0
# coh_metrix_port_download_dir: diretório onde os arquivos de métricas devem ser salvos
# coh_metrix_post_download_filename: nome do arquivo de métricas baixado. Por padrão o sistema Coh-Metrix-Port 3.0 salva
# 									as métricas num arquivo 'data.csv'
# enunciado_file: arquivo contendo os enunciados do dataset Codebench
# output_file: nome do arquivo de métricas de inteligibilidade.
# txt_dir: diretório onde os enunciados, após serem processados e limpos, são salvos num arquivo TXT
config = {
	'cwd': os.getcwd(),
	'url_pattern': r"""(?i)\b((?:https?:(?:/{1,3}|[a-z0-9%])|[a-z0-9.\-]+[.](
	?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae
	|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd
	|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr
	|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm
	|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq
	|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa
	|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm
	|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)/)(?:[^\s()<>{}\[\]]+|\([
	^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\))+(?:\([^\s()]*?\([^\s()]+\)[^\s()]*?\)|\([^\s]+?\)|[^\s`!()\[\]{
	};:'".,<>?«»“”‘’])|(?:(?<!@)[a-z0-9]+(?:[.\-][a-z0-9]+)*[.](
	?:com|net|org|edu|gov|mil|aero|asia|biz|cat|coop|info|int|jobs|mobi|museum|name|post|pro|tel|travel|xxx|ac|ad|ae
	|af|ag|ai|al|am|an|ao|aq|ar|as|at|au|aw|ax|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cd
	|cf|cg|ch|ci|ck|cl|cm|cn|co|cr|cs|cu|cv|cx|cy|cz|dd|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|eu|fi|fj|fk|fm|fo|fr
	|ga|gb|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gq|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|je|jm
	|jo|jp|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq
	|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nu|nz|om|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|ps|pt|pw|py|qa
	|re|ro|rs|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|Ja|sk|sl|sm|sn|so|sr|ss|st|su|sv|sx|sy|sz|tc|td|tf|tg|th|tj|tk|tl|tm
	|tn|to|tp|tr|tt|tv|tw|tz|ua|ug|uk|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|yu|za|zm|zw)\b/?(?!@)))""",
	'latex_patterns': [
		r'\\\[(?:.+?)\\\]',
		r'\\\((?:.+?)\\\)',
		r'\\frac{[\w\d_]+}{[\w\d_]+}'
	],
	'numerical_patterns': [
		r'(\d+(?:,\d+)?%)',
		r'(\d+(?:.\d{3})?(?:,\d+)?)'
	],
	'currency_patterns': [
		r'r\$\s?\d{1,3}(?:\.\d{3})*(?:,\d+)'
	],
	'driver_file': f'{os.getcwd()}/chromedriver_linux64/chromedriver',
	'coh_metrix_port_url': 'http://fw.nilc.icmc.usp.br:23380/cohmetrixport',
	'coh_metrix_port_download_dir': f'{os.getcwd()}/enunciados/coh-metrix',
	'coh_metrix_port_download_filename': 'data.csv',
	'enunciado_file': f'{os.getcwd()}/datasets/enunciados.csv',
	'output_file': f'{os.getcwd()}/datasets/enunciado-metricas.csv',
	'txt_dir': f'{os.getcwd()}/enunciados/txt'
}


def tag_visible(element) -> bool:
	"""
	Verifica se um elemento html é renderizável numa página html.

	:param element: objeto beautiful soup
	:return: True se element é um elemento html renderizável numa paǵina html, caso contrário retorna False.
	"""
	if element.parent.name in ['style', 'script', 'head', 'title', 'meta', '[document]']:
		return False
	if isinstance(element, Comment):
		return False
	return True


def text_from_html(html: str) -> str:
	"""
	Extrai o texto de todos os elementos num código html.

	:param html: String com o código html.
	:return: String contendo somente o texto visível do código html (sem tags).
	"""
	soup = BeautifulSoup(html, 'html.parser')
	elements = soup.findAll(text=True)
	return " ".join(element.strip() for element in elements)


def replace_by_patterns(patterns: List[str], replace: str, text: str) -> str:
	"""
	Retorna um string obtida pela substituição de todas as ocorrências de padrões regex por uma string de substituição.

	:param patterns: Lista de padrões regex a serem buscados na string "text".
	:param replace: String de substituição.
	:param text: String com o texto a ser substituído.
	:return:
	"""
	for pattern in patterns:
		text = re.sub(pattern, replace, text, flags=re.I)
	return text


def clean_enunciado(html: str) -> str:
	"""
	Processamento e limpa uma string html extraindo o enunciado de um questão.

	:param html: String html com o enunciado.
	:return: String com o texto extraído da string html.
	"""
	text = text_from_html(html)
	text = replace_by_patterns([config['url_pattern']], '', text.lower())
	text = replace_by_patterns(config['latex_patterns'], ' latex ', text)
	text = replace_by_patterns(config['currency_patterns'], ' BRL ', text)
	text = replace_by_patterns(config['numerical_patterns'], ' número ', text)
	text = re.sub(r"\s+", " ", text)
	return text.strip()


def create_options() -> Options:
	"""
	Cria um objeto com as opções de configuração para o webdriver.
	:return: Objeto com as configurações.
	"""
	options = Options()
	options.add_argument("--headless")  # comment to disable ui and speed up task
	options.add_argument("--window-size=1920x1080")
	options.add_argument("--disable-notifications")
	options.add_argument('--no-sandbox')
	options.add_argument('--verbose')
	options.add_experimental_option("prefs", {
		"download.default_directory": config['coh_metrix_port_download_dir'],
		"download.prompt_for_download": False,
		"download.directory_upgrade": True,
		"safebrowsing_for_trusted_sources_enabled": False,
		"safebrowsing.enabled": False
	})
	options.add_argument('--disable-gpu')
	options.add_argument('--disable-software-rasterizer')
	return options


def remove_special_chars(text: str):
	"""
	Substitui caracteres especiais e pontuação numa string.
	:param text: String com o texto.
	:return: String com pontuações e caracteres especiais substituídos.
	"""
	caracteres = set(re.sub(r"[\w\dáéíóúàèìòùâêîôûãẽĩõũç]", "", text, flags=re.I))
	for c in caracteres:
		if c.encode('unicode-escape') in to_replace:
			text = text.replace(c, to_replace[c.encode('unicode-escape')])
	text = re.sub(r"\s+", " ", text, flags=re.I)
	return text.strip()


def rename_downloaded_file(original_file: str, new_file: str):
	"""
	Aguarda o download o arquivo de métricas e renomeia-o.
	:param original_file: Fullpath do arquivo de métricas gerado pelo Coh-Metrix-Port 3.0 e que foi baixado.
	:param new_file: Novo fullpath do arquivo.
	"""
	while not os.path.exists(original_file):
		time.sleep(1)

	if os.path.isfile(original_file):
		os.rename(original_file, new_file)
		print(f'coh metrix port file was saved into: {new_file}')
	else:
		raise ValueError(f"coh metrix file not found: {original_file}!")


def merge_metrix_files(path: str, merged_filename: str):
	"""
	Combina todos os arquivos de métricas gerados pelo Coh-Metrix-Port 3.0 que encontram-se num diretório informado.

	:param path: Caminho do diretório onde encontram-se os arquivos de métricas.
	:param merged_filename: Nome do arquivo CSV a ser gerado.
	"""
	df = None
	# escaneia o diretório com os arquivos de métricas
	with os.scandir(path) as entries:
		for entry in entries:
			# carrega as métricas num DataFrame
			df_cohmetrix = pd.read_csv(entry.path)
			# remove as colunas referentes a numeração e o grupo das métricas
			df_cohmetrix.drop(columns=['Unnamed: 0', 'Group'], inplace=True)
			# cria o DataFrame que vai combinar as métricas
			# como índice do DataFrame utilizamos o título (nome) das métricas
			if df is None:
				df = pd.DataFrame(index=df_cohmetrix.Metric)
			# cria uma nova coluna no DataFrame utilizando como label o id da questão
			# essa coluna recebe as métricas
			df[entry.name.replace('.csv', '')] = df_cohmetrix.Value.values
	# faz a transposição do DataFrame
	# ___________________________________________			_____________________________________________
	# | index (row)		|	column (features)	|			| index (samples)	|	column (features) 	|
	# -------------------------------------------			---------------------------------------------
	# |	métricas 		| 		ids				|	==>		|	ids				|	métricas			|
	# -------------------------------------------			---------------------------------------------
	df = df.T
	# salva o DataFrame no arquivo
	df.to_csv(merged_filename)


if __name__ == '__main__':

	# diretório onde devem ser salvos os arquivos TXT com os enunciados
	txt_dir = config['txt_dir']
	# diretório de download dos arquivos de métricas gerados pelo Coh-Metrix-Post 3.0
	download_dir = config['coh_metrix_port_download_dir']
	# nome default do arquivo de métricas
	filename = config['coh_metrix_port_download_filename']

	# carrega o driver e iniciar uma instância do navegador
	browser = webdriver.Chrome(options=create_options(), executable_path=config['driver_file'])
	# carrega a paǵina de submissão do Coh-Metrix-Post 3.0
	browser.get(config['coh_metrix_port_url'])

	# abre o arquivo de dados com os enunciados.
	with open(config['enunciado_file'], 'r') as exercicios:
		# lê o arquivo de dados e separa os enunciados de questões numa lista
		questoes = ''.join(exercicios.readlines()).split('#!#!#')
		for q in questoes:
			# o if abaixo serve apenas para testar se chegamos ao final do arquivo de dados
			if len(q.strip()) > 0:
				# separa o código e enunciado da questão
				id_questao, enunciado = q.split('#;#;#')
				id_questao = id_questao.strip()

				# verifica se o arquivo de métricas dessa questão já foi baixado
				# como o processo de análise do enunciado é demorado
				# esse if permite que esse script seja finalizado e reiniciado posteriormente
				# e dessa forma ele irá continuar o download das métricas apenas para aqueles enunciados
				# que ainda não foram processados.
				if not os.path.exists(f'{download_dir}/{id_questao}.csv'):

					# limpa as tags html do enunciado
					enunciado = clean_enunciado(enunciado)
					# remove pontuação e caracteres especiais do enunciado
					enunciado = remove_special_chars(enunciado)

					# salva o enunciado já processado
					with open(f'{txt_dir}/{id_questao}.txt', 'w') as arq:
						arq.write(enunciado)
						print(f'question statement was saved into: {txt_dir}/{id_questao}.txt')

					# recupera o objeto TextArea na página do Coh-Metrix-Port
					# esse objeto deve receber o enunciado da questão
					inputText = browser.find_element_by_id("text")
					if inputText:
						# limpa qualquer conteúdo que esteja no TextArea
						inputText.clear()
						# insere o enunciado da questão
						inputText.send_keys(enunciado)
						# faz a submissão do enunciado
						inputText.submit()
					else:
						raise EnvironmentError(f'error: text area not found in page: {id_questao}')

					# Recupera o objeto Button para download do arquivo de métricas
					exportButton = browser.find_element_by_id("export")
					if exportButton:
						# efetua o "click" no button para download do arquivo
						exportButton.click()
						rename_downloaded_file(f'{download_dir}/{filename}', f'{download_dir}/{id_questao}')
					else:
						raise EnvironmentError(f'error: could not find export button: {id_questao}')

	# fecha a instância do navegador
	browser.close()

	# junta todos os arquivos de métricas num único arquivo CSV
	merge_metrix_files(download_dir, config['output_file'])
