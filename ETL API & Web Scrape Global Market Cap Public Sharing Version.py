from bs4 import BeautifulSoup	# for web scrape.
import requests			# for API call.
import pandas as pd
from datetime import datetime	# for logging function.

# super_secret_api_key = ''

def ex_rate_extraction():
	""" 
	Extract exchange rates using API from exchangeratesapi.io, convert into json, then use parts of the JSON data to construct a
	pandas DataFrame (DF) with the index being the Region abbreviation.
	"""
	url = f"http://api.exchangeratesapi.io/v1/latest?base=EUR&access_key={super_secret_api_key_here}" 
	response = requests.get(url)
	raw_data = response.json()
	ex_rate_df = pd.DataFrame(columns=['Rate'], data=raw_data['rates'].values(), index=raw_data['rates'].keys())

	return ex_rate_df


def banks_market_extraction():
	"""
	Web scrape Wikipedia's HTML table: list of largest banks by market capitalization, use beautiful soup to parse the HTML text,
	then use for loop to extract the bank name and market cap from HTML data, procuring them into a pandas DataFrame.
	"""
	html = 'https://en.wikipedia.org/wiki/List_of_largest_banks?utm_medium=Exinfluencer&utm_source=Exinfluencer&utm_content=000026UJ&utm_term=10006555&utm_id=NA-SkillsNetwork-Channel-SkillsNetworkCoursesIBMDeveloperSkillsNetworkPY0221ENSkillsNetwork23455645-2021-01-01'
	html_data = requests.get(html).text
	soup = BeautifulSoup(html_data, 'html.parser')


	bank_market_df = pd.DataFrame(columns=["Name", "Market Cap (US$ Billion)"]) # initilize empty DF containing Name & Market Cap columns.

	for row in soup.find_all('tbody')[3].find_all('tr'): # navigate through HTML's table-related tags to go to the table by market capitalization.
		col = row.find_all('td')
    
		if (col != []): # skip blank cells.
			bank = col[1].text.strip() # .text to return string and .strip() to remove leading/trailing chars
			market_cap = col[2].text.strip()
			bank_market_df = bank_market_df.append({'Name': bank, 'Market Cap (US$ Billion)': market_cap}, ignore_index=True)

	# convert values in Market Cap column to numeric since (at least in my Python environment) they were appended as object type.
	bank_market_df['Market Cap (US$ Billion)'] = bank_market_df['Market Cap (US$ Billion)'].apply(pd.to_numeric)

	return bank_market_df


def extract():
	"""
	Returns both DFs: exchange rates & list of largest banks by market capitalization, by calling
	the previously created API & web scrape functions.
	"""
	exchange_rates_df = ex_rate_extraction()
	raw_df = banks_market_extraction()

	return exchange_rates_df, raw_df


def transform(ex_df, raw_df):
	"""
	Converts and renames Market Cap column from ingested banks DF by accepting exchange rates DF and ingested banks DF.
	Region abbreviation variable listed below for easy change. Returns the region specified and transformed DF.
	"""
	region = 'GBP'
	exchange_rate = ex_df.loc[region].item()

	raw_df[f'Market Cap ({region}$ Billion)'] = round(raw_df['Market Cap (US$ Billion)'] * exchange_rate, 3) # adds the converted column rounded to 3 decimal places.
	raw_df = raw_df.drop(['Market Cap (US$ Billion)'], axis=1) # removes the old market cap in USD column.
	return region, raw_df


def load(df):
	"""
	Accepts transformed DF and exports as ready-to-load CSV (I didn't have an RDBMS database at the time of this project).
	"""
	df.to_csv(f'bank_market_cap_{region}.csv', index=False) 


def log(message):
	"""
	Accepts string message and concats with the timestamp, appending log in a text file.
	"""
	timestamp = datetime.now().strftime('%H:%M:%S-%B-%d-%Y') # Hour-Minute-Second-MonthName-Day-Year format
	with open ('ETL_Largest_Banks_logs.txt', 'a') as loggy:
		loggy.write(timestamp + ', ' + message + '\n') # \n for newline after log is added.




# ETL run:

log('ETL job started.')
log('Extract phase started.')
exchange_df, extracted_raw_df = extract()
log('Extract phase ended.')

print('Before transformation:')	# pt. 1 - quick view of data before transformation.
print(extracted_raw_df.head(1)) 

log('Transform phase started.')
region, transformed_df = transform(exchange_df, extracted_raw_df)
log('Transform phase ended.')

log('Load phase started.')
load(transformed_df)
log('Load phase ended.')

print('Post transformation:')	# pt. 2 - quick view of data after transformation.
print(transformed_df.head(1))

log('ETL job complete.')
