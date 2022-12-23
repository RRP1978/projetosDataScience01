# from operator import index
import sqlite3
import pandas as pd

conn = sqlite3.connect('web.db')

# Importa dados do bd_data.csv
df_data = pd.read_csv('bd_data.csv', index_col=0)
df_data.index.name = 'index_name'  # Altera o nome do index
df_data.to_sql('data', conn, index_label='index_name')
# Cria uma tabela de nome data, com um index chamado index_name,
# através da conexão conn.

c = conn.cursor()
c.execute('CREATE TABLE products2 (product_id, product_name, price)')
conn.commit()

c.execute('DROP TABLE products')
c.execute('DROP TABLE data')

c.execute(
    'CREATE TABLE products ([product_id] INTEGER PRIMARY KEY, [product_name] TEXT, [price] INTEGER)')


# INSERT
c.execute('''INSERT INTO products (product_id, product_name, price)
    VALUES
    (1, 'Computer', 800),
    (2, 'Printer', 200),
    (3, 'Tablet', 300)
''')
conn.commit()

df_data2 = df_data.iloc[::-2]
# Retorna erro pq tab data já existe.
df_data2.to_sql('data', conn, if_exists='append')
# Sempre atentar à engine de conexão, para não inserir dados em outra base/tabela.
# no método "to_sql", if_exists se utiliza quando a tabela pode já existir, pois retornará um erro.
# Os parâmetros podem ser fail, replace ou append.
# Replace => dropa a tabela antiga e cria a nova com os dados novos.
# append => insere os dados ao final da tabela existente.


# SELECT
c.execute("SELECT * FROM data")  # Executa a consulta e armazena.
# Traz apenas a 1ª linha da tabela, da consulta da linha anterior.
c.fetchone()
c.fetchall()  # Traz tudo que foi filtrado na linha da consulta.
df = pd.DataFrame(c.fetchall())

df  # => Não traz nada, pois a 1ª execução de fetchall, limpa a tabela.

c.execute("SELECT * FROM data WHERE A > 200")
df = pd.DataFrame(c.fetchall())
df

c.execute("SELECT * FROM data WHERE A > 200 AND B > 100")
df = pd.DataFrame(c.fetchall())
df

c.execute("SELECT A, B, C FROM data WHERE A > 200 AND B > 100")
df = pd.DataFrame(c.fetchall())


query = "SELECT * FROM data"
df = pd.read_sql(query, con=conn, index_col='index_name')
df

df = pd.read_sql(
    "SELECT A, B, C FROM data WHERE A > 200 AND B > 100", con=conn)


# UPDATE e DELETE
c.execute("UPDATE data SET A=218 WHERE index_name='b'")
conn.commit()

c.execute("UPDATE data SET A=220, B=228 WHERE index_name='b'")
conn.commit()

c.execute("DELETE FROM data WHERE index_name=1")
conn.commit()
