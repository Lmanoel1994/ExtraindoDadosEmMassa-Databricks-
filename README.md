# ExtraindoDadosEmMassa(Databricks)
**1- Criando a conexão com o Banco Sql Server**  <br />

````
def banco (tb):
    database = "Database"
    table = tb 
    user = "Usuario"
    password  = "Senha"
````

    df = spark.read\.format("jdbc") \
    .option("url", f"jdbc:sqlserver://Nome_do_Servidor:1433;databaseName={database};") \
    .option("dbtable", table) \
    .option("user", user) \
    .option("password", password) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .load()
    return df
<br />


**2- Sys.tables?!** <br />
**Sys.tables** é uma tabela padrão do Sql Server onde contem informações de todas as tabelas referente a database conectada <br />
Criando um Dataframe com a tabela **sys.tables** e criando uma lista com a coluna **name** <br />

````
df = banco('sys.table')
listaTBS = df.rdd.map(lambda x: x.name).collect()
````
<br />



3- **Criando um Lista de DataFrames**
Inserindo dentro de uma lista todas as **Tabelas do Banco **

````
listaDF = []
for i in listaTBS:
    listaDF.append(banco(i))
    print('Tabela:',i)

````

4- **Salvando os Dataframes no Azure Data Lake**

````
count = 0
for i in listaDF:
    print('Salvando Tabela: {}'.format(listaTBS[count]))
    i.repartition(1).write.format("parquet").option("header", "true").save("adl://DataLake/Tabelas/{}"\
    .format(listaTBS[count]))
    file = dbutils.fs.ls('adl://DataLake/Tabelas/{}/'.format(listaTBS[count]))[-1].path
    dbutils.fs.mv(file,"adl://DataLake/Tabelas/")
    file = file=dbutils.fs.ls('adl://DataLake/Tabelas/')[-1].path 
    dbutils.fs.mv(file, 'adl://DataLake/Tabelas/{}.parquet'.format(listaTBS[count])) 
    dbutils.fs.rm('adl://DataLake/Tabelas/{}/'.format(listaTBS[count]), recurse=True) 
    count = count +1
    
````
<br />

![alt text](https://github.com/Lmanoel1994/ExtraindoDadosEmMassa-Databricks-/blob/main/Imagens/Datalake.png) <br />
