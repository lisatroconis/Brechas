
# -*- coding: utf-8 -*-
"""
Algotimo de las bases de datos de brechas 2021 y 2022

"""

## Carga de Librerías

from IPython import get_ipython
get_ipython().magic('reset -sf')
import os
import pandas as pd
import numpy as np

# Selección de directorio
os.chdir("C:/Users/Lisa/Documents/Bases de Python/Dashboard de Brechas SAN")

## Carga de bases de datos

# La base de datos de activity info es la tabla de datos otorgada por GIFMM en versiób bruta
Ac_In = pd.read_excel("C:/Users/Lisa/Documents/Bases de Python/Dashboard de Brechas SAN/5W_Colombia_-_RMRP_2022_Consolidado uno_18032022 (6).xlsx", sheet_name='Reporte socios GIFMM_5W Colombi')

# Base de datos para divipolas y nombres de los departamentos
divla = pd.read_excel("C:/Users/Lisa/Documents/Bases de Python/Dashboard de Brechas SAN/divipolita.xlsx", sheet_name='etiqueta')
divnm = pd.read_excel("C:/Users/Lisa/Documents/Bases de Python/Dashboard de Brechas SAN//divipolita.xlsx", sheet_name='codigo')

# Bases GIFMM
PINx = pd.read_excel("C:/Users/Lisa/Documents/Bases de Python/Dashboard de Brechas SAN/PIN 2021.xlsx")
MTAx = pd.read_excel("C:/Users/Lisa/Documents/Bases de Python/Dashboard de Brechas SAN/Meta 2021.xlsx")
#FIN21 = pd.read_excel("../(0) Bases de datos/Bases productos/Financiacion.xlsx")

# Identificación de divipolas

# En esta sección se genera un algoritmo para la codificación de las unidades 
# geográficas, con base a los nombres de los departamentos

# Esta bases de datos buscan asignar el codigo divipola más adecuado dado los nombres 
# de los departamentos de la base de datos
divdp = divla[['dpto','Departamento']]
divdp = divdp.drop_duplicates()

# Esta sección busca tener una base de datos con el nombre correcto de los departamentos
divdt = divnm[['dpto','Departamento','longitud','latitud']]
divdt = divdt.drop_duplicates()

## Activity Info - 5w

# Se seleccionan las actividades del RMRP
Ac_In = Ac_In[Ac_In['Actividad RMRP'] == 'Sí']

# Selección de variables del Activity Info
Ac_In = Ac_In[['Socio Principal Nombre','Admin Departamento',
               '_ Sector','Total beneficiarios nuevos durante el mes',
              'Con vocación de permanencia', 'En tránsito','Pendulares',
              'Colombianos retornados', 'Comunidades de acogida']]

# Filtrar información para SAN
Ac_In = Ac_In[(Ac_In['_ Sector'] == 'Nutrición') | (Ac_In['_ Sector'] == 'Seguridad_Alimentaria') ]

# Renombrar variables de la base de datos
Ac_In.columns = ['Socio','Departamentos','Sector','Total 5w',
                 'Vocación de permanencia', 'En tránsito', 'Pendulares',
                 'Colombianos retornados', 'Comunidades de acogida']

Ac_In['Total 5w'] = Ac_In['Total 5w'].replace(np.nan, 0)
Ac_In['Vocación de permanencia'] = Ac_In['Vocación de permanencia'].replace(np.nan, 0)
Ac_In['En tránsito'] = Ac_In['En tránsito'].replace(np.nan, 0)
Ac_In['Pendulares'] = Ac_In['Pendulares'].replace(np.nan, 0)
Ac_In['Colombianos retornados'] = Ac_In['Colombianos retornados'].replace(np.nan, 0)
Ac_In['Comunidades de acogida'] = Ac_In['Comunidades de acogida'].replace(np.nan, 0)

## Estandarización del nombre del departamento

# Como en las bases de datos del 5w, el nombre del departamento puede estar escrito
# de diferentes maneras, se genera una función que estandarice el texto, para 
# cualquier caso

def standardize_territories(column):
    column = column.str.replace("_"," ", regex=True)
    column = column.map(lambda x: x.lower())
    column = column.map(lambda x: x.strip())
    column = column.str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8')
    column = column.str.replace(r'[^\w\s]+', '', regex=True)
    return column

# Estandarizando nombres de departamentos ...
Ac_In['Departamentos'] = standardize_territories(Ac_In['Departamentos'])


# Adicionar el divipola más adecuado
Ac_In = pd.merge(Ac_In, divdp, how= 'left', left_on = 'Departamentos',
                 right_on = 'Departamento')

# Ver las columnas de Ac_In luego del merge
Ac_In.columns

# Eliminar variables sobrantes
Ac_In = Ac_In.drop(['Departamentos','Departamento'], 1)

# Verificar resultado de identificación del divipola del departamento

if Ac_In['dpto'].isna().sum() > 1:
    print('Ajustar Divipola dpto')

## Estandarización de la base de datos

# Modificar valores de la variable Sector
Ac_In['Sector'] = Ac_In['Sector'].str.replace("_"," ")


Ac_In['Sector'] = np.select(
    [
     Ac_In['Sector'].eq('Seguridad Alimentaria'),
     Ac_In['Sector'].eq('Nutrición')
    ],
    [
     'Seguridad alimentaria',
     'Nutrición'
    ], 
    default='Sin información'
)

## Ver que solo esten esos dos sectores
Ac_In["Sector"].unique()


# Selección final de variables
Ac_In = Ac_In[['Sector','Socio','dpto','Total 5w',
               'Vocación de permanencia', 'En tránsito', 'Pendulares',
               'Colombianos retornados', 'Comunidades de acogida']]

# Crear variable Exceso
Ac_In.insert(9,'Exceso', 0, True )
Ac_In['Exceso'] = Ac_In['Total 5w'] - (Ac_In['Vocación de permanencia'] + Ac_In['En tránsito'] + 
                                       Ac_In['Pendulares'] +  Ac_In['Colombianos retornados'] + 
                                       Ac_In['Comunidades de acogida'] ) 

################### que ocurre si esta variable no da cero

## Pivotear base de datos (Pasar a estrutura larga)
# Generación de Id para pivote
Ac_In.insert(10,'id',Ac_In.index  + 1 , True )

# Selección de variables para pivotear
Ac_In_r = Ac_In[['id','Vocación de permanencia','En tránsito','Pendulares'
                 ,'Colombianos retornados','Comunidades de acogida','Exceso']]

# Pivotear base de datos
Ac_In_r = pd.melt(Ac_In_r, id_vars='id', value_vars=['Vocación de permanencia','En tránsito','Pendulares'
                 ,'Colombianos retornados','Comunidades de acogida','Exceso'])

# Organizar base de datos por id
Ac_In_r.sort_values(by = ['id','variable'], inplace = True)

# Adicionar información pivoteada
Ac_In = pd.merge(Ac_In_r, Ac_In, how= 'left', on = 'id')

#borrar la variable crada
del Ac_In_r

# Selección de variables después del pivote
Ac_In = Ac_In[['Sector','Socio','dpto','variable','value']]
Ac_In.rename(columns={"variable": "Grupo poblacional", "value": "Total 5w"}, inplace=True)

# Verificar calculo de los pivotes
Ac_In = Ac_In.groupby(['Sector', 'dpto', 'Grupo poblacional'])["Total 5w"].sum().reset_index()

Ac_In.drop_duplicates()

#--------------------------------------------------------------------------------#

## PiN 2021

# Renombrar variables
#PIN21.rename(columns={"Vocación en permanencia": "Vocación de permanencia"}, inplace=True)


## Pivotear base de datos (Pasar a estrutura larga)
# Generación de Id para pivote
PINx.insert(8,'id',PINx.index + 1, True )

# Selección de variables para pivotear
PINx_r = PINx[['id','Colombianos retornados', 'Comunidades de acogida',
       'En tránsito', 'Pendulares', 'Vocación de permanencia']]

# Pivotear base de datos
PINx_r = pd.melt(PINx_r, id_vars='id', value_vars=['Vocación de permanencia','En tránsito','Pendulares'
                 ,'Colombianos retornados','Comunidades de acogida'])

# Organizar base de datos por id
PINx_r.sort_values(by = ['id','variable'], inplace = True)
# Adicionar información pivoteada
PINx = pd.merge(PINx_r, PINx, how= 'left', on = 'id')

del PINx_r

# Selección de variables para la base final
PINx = PINx[['Sector', 'dpto','variable', 'value']]
PINx.rename(columns={"variable": "Grupo poblacional", "value": "Total PIN 2021"}, 
             inplace=True)

#--------------------------------------------------------------------------------#
## Meta 2021

# Renombrar variables
MTAx = MTAx.drop('Departamento', 1)

## Pivotear base de datos (Pasar a estrutura larga)
# Generación de Id para pivote
MTAx.insert(7,'id',MTAx.index + 1, True )

# Selección de variables para pivotear
MTAx_r = MTAx[['id','Colombianos retornados', 'Comunidades de acogida',
       'En tránsito', 'Pendulares', 'Vocación de permanencia']]

# Pivotear base de datos
MTAx_r = pd.melt(MTAx_r, id_vars='id', value_vars=['Vocación de permanencia','En tránsito','Pendulares'
                 ,'Colombianos retornados','Comunidades de acogida'])

# Organizar base de datos por id
MTAx_r.sort_values(by = ['id','variable'], inplace = True)
# Adicionar información pivoteada
MTAx = pd.merge(MTAx_r, MTAx, how= 'left', on = 'id')

# Selección de variables para la base final
MTAx = MTAx[['Sector', 'dpto','variable', 'value']]
MTAx.rename(columns={"variable": "Grupo poblacional", "value": "Total Meta 2021"}, 
             inplace=True)
#--------------------------------------------------------------------------------#
## Unión de bases de datos

# Generación de base matriz
base_dpto = divdt[['dpto']]

# Adicionar las fuentes de información utilizadas
Ac_In = pd.merge(Ac_In, base_dpto, how= 'outer', on = 'dpto')
Ac_In = pd.merge(Ac_In, PINx, how= 'outer', on = ['Sector','dpto','Grupo poblacional'])
Ac_In = pd.merge(Ac_In, MTAx, how= 'outer', on = ['Sector','dpto','Grupo poblacional'])
#Ac_In = pd.merge(Ac_In, FIN21, how= 'left', on = 'Sector')

# Adicionar el nombre correcto de los departamentos
Ac_In = pd.merge(Ac_In, divdt, how= 'left', on = 'dpto')

# Ajustes para la sección de financiamiento
#Ac_In.insert(8,'Avance financiación', Ac_In['Financiación']/Ac_In['Meta Financiación'], True )
#Ac_In.insert(9,'Diferencia Financiación', Ac_In['Meta Financiación'] - Ac_In['Financiación'], True )
#Ac_In['Diferencia Financiación'] = Ac_In['Diferencia Financiación'].apply(lambda x: 0 if x < 0 else x)


# Estandarización de valores pérdidos
########################################## ver 
Ac_In = Ac_In.dropna(subset=['Sector'])
Ac_In = Ac_In.fillna(0)

# Remover departamentos sin ninguna información
Ac_In['Control'] = Ac_In['Total 5w'] + Ac_In['Total PIN 2021'] + Ac_In['Total Meta 2021']

Ac_In = Ac_In[Ac_In['Control'] > 0]

# Selección de variables finales

Ac_In = Ac_In[['Sector','dpto','Departamento','longitud','latitud','Grupo poblacional',
               'Total 5w','Total PIN 2021','Total Meta 2021']]#'Financiación',
               #'Meta Financiación','Avance financiación', 'Diferencia Financiación'


#Ac_In.rename(columns={"Financiacción": "Financiación"}, inplace=True)

# Exportar base final
Ac_In.to_excel("C:/Users/Lisa/Documents/Bases de Python/Dashboard de Brechas SAN/base_brechas_2021.xlsx",index=False)
