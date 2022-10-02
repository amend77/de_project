#!/usr/bin/python

import pandas as pd
import jaydebeapi
import os
import datetime as dt

# Функция обработки SQL из файла
def open_file(path):
	f = open(path,'r',encoding="utf-8")
	sql = f.read()
	for i in sql.split(';'):
		if i.strip() != '':
			curs.execute(i)

# Функция обработки XLSX файлов в SQL
def xlsx_to_sql(path, sql_script):
	df = pd.read_excel( path , header=0, index_col=None )
	df = df.astype(str)
	curs.executemany( sql_script , df.values.tolist() )
	conn.commit()

# Функция обработки SQL из файла + замена даты
def open_file_and_replace(path, replace, time):
	f = open(path,'r',encoding="utf-8")
	sql = f.read()
	time = str(time)
	sql = sql.replace(replace, time[:10])
	for i in sql.split(';'):
		if i.strip() != '':
			curs.execute(i)


# Подключение к серверу
conn = jaydebeapi.connect( 
'oracle.jdbc.driver.OracleDriver',
'jdbc:oracle:thin:de1m/samwisegamgee@de-oracle.chronosavant.ru:1521/deoracle',
['de1m','samwisegamgee'],
'/home/de1m/mndg/ojdbc8.jar'
)

curs = conn.cursor()
conn.jconn.setAutoCommit(False)

# Поиск файлов в папке data

for r in os.listdir('/home/de1m/mndg/data/'):
	if os.path.isfile('/home/de1m/mndg/data/'+r):
		name, ext = os.path.splitext(r) 
		if name[:-9] == 'passport_blacklist':
			psbl = r
		if name[:-9] == 'terminals':
			terminals = r
			timef = dt.datetime.strptime(str(name.split('_')[-1]), '%d%m%Y')
		if name[:-9] == 'transactions':
			transactions = r
		

# Загрузка passport_blacklist из файла XLSX

xlsx_to_sql('/home/de1m/mndg/data/'+psbl, "insert into de1m.mndg_stg_pssprt_blcklst ( entry_dt, passport_num ) values ( to_date( ?, 'YYYY-MM-DD' ), ?)")

# Обработка passport_blacklist в scd2

open_file('/home/de1m/mndg/sql_scripts/sql_bl.sql')

# Загрузка terminals из файла XLSX 

xlsx_to_sql('/home/de1m/mndg/data/'+terminals, "insert into de1m.mndg_stg_terminals ( terminal_id, terminal_type, terminal_city, terminal_address ) values ( ?, ?, ?, ? )")

# Обработка terminals в scd2

open_file_and_replace('/home/de1m/mndg/sql_scripts/sql_terminals.sql', '@file', timef)

# Загрузка transactions из файла txt

df = pd.read_csv( '/home/de1m/mndg/data/'+transactions, sep=';', header=0, index_col=None ) 
df['amount'] = df['amount'].str.replace(',', '.')
curs.executemany( "insert into de1m.mndg_stg_transactions ( transaction_id, transaction_date, amount, card_num, oper_type, oper_result, terminal ) values ( ?, to_date( ?, 'YYYY-MM-DD HH24:MI:SS' ), ?, ?, ?, ?, ? )", df.values.tolist() )
conn.commit()

# Обработка transactions в scd2

open_file('/home/de1m/mndg/sql_scripts/sql_transactions.sql')


# Обработка accounts в scd2 

open_file('/home/de1m/mndg/sql_scripts/sql_accounts.sql')


# Обработка cards в scd2

open_file('/home/de1m/mndg/sql_scripts/sql_cards.sql')

# Обработка clients в scd2

open_file('/home/de1m/mndg/sql_scripts/sql_clients.sql')

# Построение отчета по мошенническим операциям

open_file_and_replace('/home/de1m/mndg/sql_scripts/rep_fraud.sql', '@file', timef)


# Отправляем файлы в архив

if 'psbl' in locals() and 'terminals' in locals() and 'transactions' in locals():
	os.rename('/home/de1m/mndg/data/'+ psbl, '/home/de1m/mndg/data/archive/'+psbl+'.backup')
	os.rename('/home/de1m/mndg/data/'+ terminals, '/home/de1m/mndg/data/archive/'+terminals+'.backup')
	os.rename('/home/de1m/mndg/data/'+ transactions, '/home/de1m/mndg/data/archive/'+transactions+'.backup')
