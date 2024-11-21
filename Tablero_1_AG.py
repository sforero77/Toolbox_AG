from sqlalchemy import create_engine, Column, Integer, String, Numeric, DateTime, ForeignKey, Float, and_, literal,or_
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from sqlalchemy  import func
from arcgis.gis import GIS
import pandas as pd

#arcpy.env.timeZone = "E._South_America_Standard_Time"

Base = declarative_base()
class MgnMpio(Base):
    __tablename__ = 'mgn_mpio_politico'
    __table_args__ = {'schema': 'municipios'}
    
    gid = Column(Integer)
    dpto_ccdgo = Column(String)
    mpio_ccdgo = Column(String)
    mpio_cdpmp = Column(String, primary_key=True)
    dpto_cnmbr = Column(String)
    mpio_cnmbr = Column(String)
    mpio_crslc = Column(String)
    mpio_narea = Column(Numeric)
    mpio_csmbl = Column(String)
    mpio_vgnc = Column(Integer)
    mpio_tipo = Column(String)
    shape_leng = Column(Numeric)
    shape_area = Column(Numeric)
    geom = Column(String)  
    mpio_pdet = relationship("MunicipiosPdet", back_populates="pdets")
    mpio_poblacion = relationship("CombinadoHistPobl", back_populates="poblaciones")
    mpio_policia = relationship("CombinadoHistPolicia", back_populates="policias")
class MunicipiosPdet(Base):
    __tablename__ = 'municipiospdet'
    __table_args__ = {'schema': 'municipios'}
    
    subregion_pdet = Column(String)
    codigo_dane_departamento = Column(String)
    departamento = Column(String)
    codigo_dane_municipio = Column(String, ForeignKey('municipios.mgn_mpio_politico.mpio_cdpmp'), primary_key=True, nullable=False)
    municipio = Column(String)
    pdets = relationship("MgnMpio", back_populates="mpio_pdet")
class CombinadoHistPobl(Base):
    __tablename__ = 'poblacion_dane_no_oficial'
    __table_args__ = {'schema': 'poblacion'}
    
    dp = Column(String)
    dpnom = Column(String)
    dpmp = Column(String, ForeignKey('municipios.mgn_mpio_politico.mpio_cdpmp'), primary_key=True)
    poblaciones = relationship("MgnMpio", back_populates="mpio_poblacion")
    mpio = Column(String)
    ano = Column(Integer, primary_key=True)
    area_geografica = Column(String, primary_key=True)
    hombres_0 = Column(Integer)
    hombres_1 = Column(Integer)
    hombres_2 = Column(Integer)
    hombres_3 = Column(Integer)
    hombres_4 = Column(Integer)
    hombres_5 = Column(Integer)
    hombres_6 = Column(Integer)
    hombres_7 = Column(Integer)
    hombres_8 = Column(Integer)
    hombres_9 = Column(Integer)
    hombres_10 = Column(Integer)
    hombres_11 = Column(Integer)
    hombres_12 = Column(Integer)
    hombres_13 = Column(Integer)
    hombres_14 = Column(Integer)
    hombres_15 = Column(Integer)
    hombres_16 = Column(Integer)
    hombres_17 = Column(Integer)
    hombres_18 = Column(Integer)
    hombres_19 = Column(Integer)
    hombres_20 = Column(Integer)
    hombres_21 = Column(Integer)
    hombres_22 = Column(Integer)
    hombres_23 = Column(Integer)
    hombres_24 = Column(Integer)
    hombres_25 = Column(Integer)
    hombres_26 = Column(Integer)
    hombres_27 = Column(Integer)
    hombres_28 = Column(Integer)
    hombres_29 = Column(Integer)
    hombres_30 = Column(Integer)
    hombres_31 = Column(Integer)
    hombres_32 = Column(Integer)
    hombres_33 = Column(Integer)
    hombres_34 = Column(Integer)
    hombres_35 = Column(Integer)
    hombres_36 = Column(Integer)
    hombres_37 = Column(Integer)
    hombres_38 = Column(Integer)
    hombres_39 = Column(Integer)
    hombres_40 = Column(Integer)
    hombres_41 = Column(Integer)
    hombres_42 = Column(Integer)
    hombres_43 = Column(Integer)
    hombres_44 = Column(Integer)
    hombres_45 = Column(Integer)
    hombres_46 = Column(Integer)
    hombres_47 = Column(Integer)
    hombres_48 = Column(Integer)
    hombres_49 = Column(Integer)
    hombres_50 = Column(Integer)
    hombres_51 = Column(Integer)
    hombres_52 = Column(Integer)
    hombres_53 = Column(Integer)
    hombres_54 = Column(Integer)
    hombres_55 = Column(Integer)
    hombres_56 = Column(Integer)
    hombres_57 = Column(Integer)
    hombres_58 = Column(Integer)
    hombres_59 = Column(Integer)
    hombres_60 = Column(Integer)
    hombres_61 = Column(Integer)
    hombres_62 = Column(Integer)
    hombres_63 = Column(Integer)
    hombres_64 = Column(Integer)
    hombres_65 = Column(Integer)
    hombres_66 = Column(Integer)
    hombres_67 = Column(Integer)
    hombres_68 = Column(Integer)
    hombres_69 = Column(Integer)
    hombres_70 = Column(Integer)
    hombres_71 = Column(Integer)
    hombres_72 = Column(Integer)
    hombres_73 = Column(Integer)
    hombres_74 = Column(Integer)
    hombres_75 = Column(Integer)
    hombres_76 = Column(Integer)
    hombres_77 = Column(Integer)
    hombres_78 = Column(Integer)
    hombres_79 = Column(Integer)
    hombres_80 = Column(Integer)
    hombres_81 = Column(Integer)
    hombres_82 = Column(Integer)
    hombres_83 = Column(Integer)
    hombres_84 = Column(Integer)
    hombres_85_y_más = Column(Integer)
    mujeres_0 = Column(Integer)
    mujeres_1 = Column(Integer)
    mujeres_2 = Column(Integer)
    mujeres_3 = Column(Integer)
    mujeres_4 = Column(Integer)
    mujeres_5 = Column(Integer)
    mujeres_6 = Column(Integer)
    mujeres_7 = Column(Integer)
    mujeres_8 = Column(Integer)
    mujeres_9 = Column(Integer)
    mujeres_10 = Column(Integer)
    mujeres_11 = Column(Integer)
    mujeres_12 = Column(Integer)
    mujeres_13 = Column(Integer)
    mujeres_14 = Column(Integer)
    mujeres_15 = Column(Integer)
    mujeres_16 = Column(Integer)
    mujeres_17 = Column(Integer)
    mujeres_18 = Column(Integer)
    mujeres_19 = Column(Integer)
    mujeres_20 = Column(Integer)
    mujeres_21 = Column(Integer)
    mujeres_22 = Column(Integer)
    mujeres_23 = Column(Integer)
    mujeres_24 = Column(Integer)
    mujeres_25 = Column(Integer)
    mujeres_26 = Column(Integer)
    mujeres_27 = Column(Integer)
    mujeres_28 = Column(Integer)
    mujeres_29 = Column(Integer)
    mujeres_30 = Column(Integer)
    mujeres_31 = Column(Integer)
    mujeres_32 = Column(Integer)
    mujeres_33 = Column(Integer)
    mujeres_34 = Column(Integer)
    mujeres_35 = Column(Integer)
    mujeres_36 = Column(Integer)
    mujeres_37 = Column(Integer)
    mujeres_38 = Column(Integer)
    mujeres_39 = Column(Integer)
    mujeres_40 = Column(Integer)
    mujeres_41 = Column(Integer)
    mujeres_42 = Column(Integer)
    mujeres_43 = Column(Integer)
    mujeres_44 = Column(Integer)
    mujeres_45 = Column(Integer)
    mujeres_46 = Column(Integer)
    mujeres_47 = Column(Integer)
    mujeres_48 = Column(Integer)
    mujeres_49 = Column(Integer)
    mujeres_50 = Column(Integer)
    mujeres_51 = Column(Integer)
    mujeres_52 = Column(Integer)
    mujeres_53 = Column(Integer)
    mujeres_54 = Column(Integer)
    mujeres_55 = Column(Integer)
    mujeres_56 = Column(Integer)
    mujeres_57 = Column(Integer)
    mujeres_58 = Column(Integer)
    mujeres_59 = Column(Integer)
    mujeres_60 = Column(Integer)
    mujeres_61 = Column(Integer)
    mujeres_62 = Column(Integer)
    mujeres_63 = Column(Integer)
    mujeres_64 = Column(Integer)
    mujeres_65 = Column(Integer)
    mujeres_66 = Column(Integer)
    mujeres_67 = Column(Integer)
    mujeres_68 = Column(Integer)
    mujeres_69 = Column(Integer)
    mujeres_70 = Column(Integer)
    mujeres_71 = Column(Integer)
    mujeres_72 = Column(Integer)
    mujeres_73 = Column(Integer)
    mujeres_74 = Column(Integer)
    mujeres_75 = Column(Integer)
    mujeres_76 = Column(Integer)
    mujeres_77 = Column(Integer)
    mujeres_78 = Column(Integer)
    mujeres_79 = Column(Integer)
    mujeres_80 = Column(Integer)
    mujeres_81 = Column(Integer)
    mujeres_82 = Column(Integer)
    mujeres_83 = Column(Integer)
    mujeres_84 = Column(Integer)
    mujeres_85_y_más = Column(Integer)
    total_0 = Column(Integer)
    total_1 = Column(Integer)
    total_2 = Column(Integer)
    total_3 = Column(Integer)
    total_4 = Column(Integer)
    total_5 = Column(Integer)
    total_6 = Column(Integer)
    total_7 = Column(Integer)
    total_8 = Column(Integer)
    total_9 = Column(Integer)
    total_10 = Column(Integer)
    total_11 = Column(Integer)
    total_12 = Column(Integer)
    total_13 = Column(Integer)
    total_14 = Column(Integer)
    total_15 = Column(Integer)
    total_16 = Column(Integer)
    total_17 = Column(Integer)
    total_18 = Column(Integer)
    total_19 = Column(Integer)
    total_20 = Column(Integer)
    total_21 = Column(Integer)
    total_22 = Column(Integer)
    total_23 = Column(Integer)
    total_24 = Column(Integer)
    total_25 = Column(Integer)
    total_26 = Column(Integer)
    total_27 = Column(Integer)
    total_28 = Column(Integer)
    total_29 = Column(Integer)
    total_30 = Column(Integer)
    total_31 = Column(Integer)
    total_32 = Column(Integer)
    total_33 = Column(Integer)
    total_34 = Column(Integer)
    total_35 = Column(Integer)
    total_36 = Column(Integer)
    total_37 = Column(Integer)
    total_38 = Column(Integer)
    total_39 = Column(Integer)
    total_40 = Column(Integer)
    total_41 = Column(Integer)
    total_42 = Column(Integer)
    total_43 = Column(Integer)
    total_44 = Column(Integer)
    total_45 = Column(Integer)
    total_46 = Column(Integer)
    total_47 = Column(Integer)
    total_48 = Column(Integer)
    total_49 = Column(Integer)
    total_50 = Column(Integer)
    total_51 = Column(Integer)
    total_52 = Column(Integer)
    total_53 = Column(Integer)
    total_54 = Column(Integer)
    total_55 = Column(Integer)
    total_56 = Column(Integer)
    total_57 = Column(Integer)
    total_58 = Column(Integer)
    total_59 = Column(Integer)
    total_60 = Column(Integer)
    total_61 = Column(Integer)
    total_62 = Column(Integer)
    total_63 = Column(Integer)
    total_64 = Column(Integer)
    total_65 = Column(Integer)
    total_66 = Column(Integer)
    total_67 = Column(Integer)
    total_68 = Column(Integer)
    total_69 = Column(Integer)
    total_70 = Column(Integer)
    total_71 = Column(Integer)
    total_72 = Column(Integer)
    total_73 = Column(Integer)
    total_74 = Column(Integer)
    total_75 = Column(Integer)
    total_76 = Column(Integer)
    total_77 = Column(Integer)
    total_78 = Column(Integer)
    total_79 = Column(Integer)
    total_80 = Column(Integer)
    total_81 = Column(Integer)
    total_82 = Column(Integer)
    total_83 = Column(Integer)
    total_84 = Column(Integer)
    total_85_y_más = Column(Integer)
    total_hombres = Column(Integer)
    total_mujeres = Column(Integer)
    total_general = Column(Integer)
class CombinadoHistPolicia(Base):
    __tablename__ = 'combinado_hist_policia'
    __table_args__ = {'schema': 'siedco'}
    
    id = Column(Integer, primary_key=True)
    departamento = Column(String)
    municipio = Column(String)
    codigo_dane = Column(String)
    armas_medios = Column(String)
    fecha_hecho = Column(DateTime)
    genero = Column(String)
    f_agrupa_edad_persona = Column(String)
    cantidad = Column(Integer)
    primeros_5_dane = Column(String, ForeignKey('municipios.mgn_mpio_politico.mpio_cdpmp'))
    primeros_dos_digitos = Column(String)
    policias = relationship("MgnMpio", back_populates="mpio_policia")
    ano_fecha_hecho = Column(Integer)
class MunicipiosCategoria(Base):
    __tablename__ = 'municipios_categoria'
    __table_args__ = {'schema': 'municipios'}

    divipola=Column(String, primary_key=True)
    cat=Column(String)
    
# Configurar la conexión a la base de datos PostgreSQL (cambia la URL según tu configuración)
# Formato: postgresql://usuario:contraseña@localhost:puerto/nombre_de_la_base_de_datos
engine = create_engine('postgresql://postgres:postgres@186.31.192.90:5432/AG_SEBAS')
# Crear una sesión para interactuar con la base de datos
Session = sessionmaker(bind=engine)
session = Session()
arcpy.AddMessage(f"""
===========================================
CTP NACIONAL
===========================================
""")
# Subconsulta para CombinadoHistPolicia: Agrupar por año y sumar la cantidad
subconsulta_policia = (
    session.query(
        func.extract('year', CombinadoHistPolicia.fecha_hecho).label('ano'),
        func.sum(CombinadoHistPolicia.cantidad).label('homicidios_conteo')
    )
    .group_by(func.extract('year', CombinadoHistPolicia.fecha_hecho))
    .subquery()
)
# Subconsulta para CombinadoHistPobl: Agrupar por año y sumar total_general donde area_geografica es igual a 'total'
subconsulta_poblacion = (
    session.query(
        CombinadoHistPobl.ano,
        func.sum(func.COALESCE(CombinadoHistPobl.total_general, 0)).label('poblacion_conteo')
    )
    .filter(CombinadoHistPobl.area_geografica == 'Total')
    .group_by(CombinadoHistPobl.ano)
    .subquery()
)
# Consulta principal: Unir las subconsultas
consulta_combinada = (
    session.query(
        func.COALESCE(subconsulta_policia.c.ano, subconsulta_poblacion.c.ano).label('ano'),
        subconsulta_policia.c.homicidios_conteo,
        subconsulta_poblacion.c.poblacion_conteo
    )
    .join(subconsulta_policia, subconsulta_poblacion.c.ano == subconsulta_policia.c.ano)
    .order_by('ano')
    .all()
)
# Convertir los resultados a un DataFrame de Pandas
df_resultados = pd.DataFrame(consulta_combinada)
df_resultados['Lugar'] = 'Colombia'
df_resultados['Metrica'] = 'Conteo'
df_resultados['Violencia'] = 'Homicidios'
df_resultados['Fuente'] = 'Policía Nacional'
df_resultados['Tasa'] = ((df_resultados['homicidios_conteo'] / df_resultados['poblacion_conteo']) * 100000).round(3)
# Ordenar el DataFrame por año para asegurar la secuencia correcta
df_resultados = df_resultados.sort_values(by='ano')
df_resultados['CambioCasos'] = df_resultados['homicidios_conteo'].diff()
df_resultados['CambioCasosPor'] = (df_resultados['homicidios_conteo'].pct_change()) * 100
df_resultados['CambioConteo'] = df_resultados['CambioCasos'].apply(lambda x: 'Menor' if x < 0 else ('Mayor' if x > 0 else 'Igual'))
# Imprimir el DataFrame
arcpy.AddMessage("Resultados en Pandas DataFrame:")
arcpy.AddMessage(df_resultados)
arcpy.AddMessage(df_resultados.columns)
# Subir el DataFrame df_resultados a la base de datos
df_resultados.to_sql(name='CTP_POLICIA', con=engine, if_exists='replace', index=False, schema='siedco')
arcpy.AddMessage(f"""
===========================================
TCP DEPARTAMENTO
===========================================
""")
# Subconsulta para CombinadoHistPolicia: Agrupar por año y primeros_5_dane, y sumar la cantidad
subconsulta_policia_dept = (
    session.query(
        func.extract('year', CombinadoHistPolicia.fecha_hecho).label('ano'),
        CombinadoHistPolicia.primeros_dos_digitos,
        CombinadoHistPolicia.departamento,
        func.sum(CombinadoHistPolicia.cantidad).label('homicidios_conteo')
    )
    .group_by(func.extract('year', CombinadoHistPolicia.fecha_hecho), CombinadoHistPolicia.primeros_dos_digitos,CombinadoHistPolicia.departamento)
    .subquery()
)
# Subconsulta para CombinadoHistPobl: Agrupar por año, departamento y sumar total_general donde area_geografica es igual a 'total'
subconsulta_poblacion_dept = (
    session.query(
        CombinadoHistPobl.ano,
        CombinadoHistPobl.dp,
        CombinadoHistPobl.dpnom,
        func.sum(func.COALESCE(CombinadoHistPobl.total_general, 0)).label('poblacion_conteo')
    )
    .filter(CombinadoHistPobl.area_geografica == 'Total')
    .group_by(CombinadoHistPobl.ano, CombinadoHistPobl.dp,CombinadoHistPobl.dpnom)
    .subquery()
)
consulta_combinada_dept = (
    session.query(
        func.COALESCE(subconsulta_policia_dept.c.ano, subconsulta_poblacion_dept.c.ano).label('ano'),
        subconsulta_policia_dept.c.homicidios_conteo,
        subconsulta_policia_dept.c.primeros_dos_digitos,
        subconsulta_poblacion_dept.c.poblacion_conteo,
        subconsulta_policia_dept.c.departamento,
    )
    .join(subconsulta_policia_dept, and_(
        subconsulta_poblacion_dept.c.ano == subconsulta_policia_dept.c.ano,
        subconsulta_poblacion_dept.c.dp == subconsulta_policia_dept.c.primeros_dos_digitos
    ))
    .order_by('ano')
    .all()
)
# Convertir los resultados a un DataFrame de Pandas
df_resultados_dept = pd.DataFrame(consulta_combinada_dept)
df_resultados_dept['Tasa'] = ((df_resultados_dept['homicidios_conteo'] / df_resultados_dept['poblacion_conteo']) * 100000).round(3)
df_resultados_dept = df_resultados_dept.sort_values(by=['ano', 'primeros_dos_digitos'])
df_resultados_dept['CambioCasos'] = df_resultados_dept.groupby('primeros_dos_digitos')['homicidios_conteo'].diff()
df_resultados_dept['CambioCasosPor'] = (df_resultados_dept.groupby('primeros_dos_digitos')['homicidios_conteo'].pct_change()) * 100
df_resultados_dept['CambioConteo'] = df_resultados_dept['CambioCasos'].apply(lambda x: 'Menor' if x < 0 else ('Mayor' if x > 0 else 'Igual'))
# Imprimir el DataFrame
arcpy.AddMessage("Resultados en Pandas DataFrame:")
arcpy.AddMessage(df_resultados_dept)
arcpy.AddMessage(df_resultados_dept.columns)
# Subir el DataFrame df_resultados a la base de datos
df_resultados_dept.to_sql(name='TCP_DEPARTAMENTO', con=engine, if_exists='replace', index=False, schema='siedco')
arcpy.AddMessage(f"""
===========================================
TCP MUNCIPIO PDET
===========================================
""")
# Subconsulta para CombinadoHistPolicia: Agrupar por año y primeros_5_dane, y sumar la cantidad
subconsulta_policia_mun = (
    session.query(
        func.extract('year', CombinadoHistPolicia.fecha_hecho).label('ano'),
        CombinadoHistPolicia.primeros_5_dane,
        CombinadoHistPolicia.municipio,
        func.sum(CombinadoHistPolicia.cantidad).label('homicidios_conteo')
    )
    .group_by(func.extract('year', CombinadoHistPolicia.fecha_hecho), CombinadoHistPolicia.primeros_5_dane,CombinadoHistPolicia.municipio)
    .subquery()
)
subconsulta_poblacion_mun = (
    session.query(
        CombinadoHistPobl.ano,
        CombinadoHistPobl.dpmp,
        CombinadoHistPobl.mpio,
        CombinadoHistPobl.dpnom,
        func.sum(func.COALESCE(CombinadoHistPobl.total_general, 0)).label('poblacion_conteo')
    )
    .group_by(CombinadoHistPobl.ano, CombinadoHistPobl.dpmp,CombinadoHistPobl.mpio,CombinadoHistPobl.dpnom)
    .filter(CombinadoHistPobl.area_geografica == 'Total')
    .subquery()
)
subconsulta_pdet_mun = (
    session.query(
        MunicipiosPdet.codigo_dane_municipio,
        literal('PDET').label('tipo')  
    )
    .subquery()
)
subconsulta_categoria_mun= (
    session.query(
        MunicipiosCategoria.divipola,
        MunicipiosCategoria.cat
    )
    .subquery()
)
consulta_combinada_mun = (
    session.query(
        func.COALESCE(subconsulta_policia_mun.c.ano, subconsulta_poblacion_mun.c.ano).label('ano'),
        subconsulta_poblacion_mun.c.dpmp,
        subconsulta_poblacion_mun.c.dpnom,
        subconsulta_policia_mun.c.homicidios_conteo,
        subconsulta_poblacion_mun.c.poblacion_conteo,
        subconsulta_poblacion_mun.c.mpio,
        subconsulta_pdet_mun.c.tipo,
        subconsulta_categoria_mun.c.cat
    )
    .join(subconsulta_poblacion_mun, and_(
        subconsulta_poblacion_mun.c.ano == subconsulta_policia_mun.c.ano, 
        subconsulta_poblacion_mun.c.dpmp == subconsulta_policia_mun.c.primeros_5_dane), full=True)
    .join(subconsulta_pdet_mun, subconsulta_pdet_mun.c.codigo_dane_municipio == subconsulta_poblacion_mun.c.dpmp, full=True)
    .join(subconsulta_categoria_mun, subconsulta_categoria_mun.c.divipola == subconsulta_poblacion_mun.c.dpmp, full=True)
    .filter(subconsulta_poblacion_mun.c.ano.between(2010, 2023))
    .order_by('ano')
    .all()
)
df_resultados_mun = pd.DataFrame(consulta_combinada_mun)

# Rellenar con ceros los valores nulos en la columna 'homicidios_conteo'
df_resultados_mun['homicidios_conteo'] = df_resultados_mun['homicidios_conteo'].fillna(0)
df_resultados_mun['concat'] =  df_resultados_mun['dpmp'].astype(str) + '-' + df_resultados_mun['ano'].astype(str) 
df_resultados_mun['Tasa'] = ((df_resultados_mun['homicidios_conteo'] / df_resultados_mun['poblacion_conteo']) * 100000).round(3)
df_resultados_mun['DepMun'] =  df_resultados_mun['dpnom'].astype(str) + '-' + df_resultados_mun['mpio'].astype(str) 
df_resultados_mun = df_resultados_mun.sort_values(by=['ano', 'dpmp'])
df_resultados_mun['CambioCasos'] = df_resultados_mun.groupby('dpmp')['homicidios_conteo'].diff()
df_resultados_mun['CambioCasosPor'] = (df_resultados_mun.groupby('dpmp')['homicidios_conteo'].pct_change()) * 100
# Reemplazar valores infinitos con NaN en la columna 'CambioCasosPor'
df_resultados_mun['CambioCasosPor'] = df_resultados_mun['CambioCasosPor'].replace([float('inf'), float('-inf')], pd.NA)
df_resultados_mun['CambioCasosPor'].replace([None, pd.NA], pd.NA, inplace=True)
df_resultados_mun['CambioConteo'] = df_resultados_mun['CambioCasos'].apply(lambda x: 'Menor' if x < 0 else ('Mayor' if x > 0 else 'Igual'))

#arcpy.AddMessage(df_resultados_mun[df_resultados_mun['dpmp'] == '99624'][['homicidios_conteo', 'CambioCasos', 'ano','CambioCasosPor']])
arcpy.AddMessage(df_resultados_mun)
arcpy.AddMessage(df_resultados_mun.columns)

df_resultados_mun.to_sql(name='TCP_CATEGORIA_PDET', con=engine, if_exists='replace', index=False, schema='siedco')

arcpy.AddMessage(f"""
===========================================
HOMICIDIOS_Variables_POLICIA
===========================================
""")
consulta_policia = (
    session.query(
        CombinadoHistPolicia.departamento,
        CombinadoHistPolicia.municipio,
        CombinadoHistPolicia.armas_medios,
        CombinadoHistPolicia.fecha_hecho,
        CombinadoHistPolicia.genero,
        CombinadoHistPolicia.f_agrupa_edad_persona,
        CombinadoHistPolicia.cantidad,
        CombinadoHistPolicia.primeros_5_dane,
        CombinadoHistPolicia.ano_fecha_hecho
    )
    .filter(CombinadoHistPolicia.primeros_5_dane.isnot(None))  # Opcional: Filtrar registros donde primeros_5_dane no sea nulo
    .all()
)

df_resultados_policia = pd.DataFrame(consulta_policia)
df_resultados_policia['dia'] = df_resultados_policia['fecha_hecho'].dt.day
df_resultados_policia['mes'] = df_resultados_policia['fecha_hecho'].dt.month
df_resultados_policia['numero_semana'] = df_resultados_policia['fecha_hecho'].dt.isocalendar().week
meses_en_espanol = {
    1: 'Enero',
    2: 'Febrero',
    3: 'Marzo',
    4: 'Abril',
    5: 'Mayo',
    6: 'Junio',
    7: 'Julio',
    8: 'Agosto',
    9: 'Septiembre',
    10: 'Octubre',
    11: 'Noviembre',
    12: 'Diciembre'
}
df_resultados_policia['nombre_mes'] = df_resultados_policia['mes'].map(meses_en_espanol)
df_resultados_policia['nombre_dia_semana'] = df_resultados_policia['fecha_hecho'].dt.strftime('%A')
df_resultados_policia['DepMun'] =  df_resultados_policia['departamento'].astype(str) + '-' + df_resultados_policia['municipio'].astype(str) 
df_resultados_policia['NumMes'] =  df_resultados_policia['mes'].astype(str) + '-' + df_resultados_policia['nombre_mes'].astype(str)
df_resultados_policia['MesAno'] =  df_resultados_policia['mes'].astype(str) + '-' + df_resultados_policia['ano_fecha_hecho'].astype(str)

arcpy.AddMessage(df_resultados_policia)
arcpy.AddMessage(df_resultados_policia.columns)

df_resultados_policia.to_sql(name='HOMICIDIOS_Variables_POLICIA', con=engine, if_exists='replace', index=False, schema='siedco')

"""arcpy.management.DeleteRows(
    in_rows="https://geoapps.esri.co/server/rest/services/Hosted/HOMICIDIOS_Variables_POLICIA2/FeatureServer/0"
)

# Define el URL del feature service
target_fc = 'https://geoapps.esri.co/server/rest/services/Hosted/HOMICIDIOS_Variables_POLICIA2/FeatureServer/0'

# Describe el feature class
dsc = arcpy.Describe(target_fc)
fields = dsc.fields
fieldnames = [field.name for field in fields]
#arcpy.AddMessage(fieldnames)


# Convierte las fechas a formato datetime y ajusta la zona horaria
df_resultados_policia['fecha_hecho'] = pd.to_datetime(df_resultados_policia['fecha_hecho'], errors='coerce')
df_resultados_policia['fecha_hecho'] += pd.Timedelta(hours=6)
#arcpy.AddMessage(df_resultados_policia['fecha_hecho'])
# Define el nuevo orden de las columnas
nuevo_orden_columnas = ['departamento', 'municipio', 'armas_medios', 'genero', 
                        'f_agrupa_edad_persona', 'cantidad', 'primeros_5_dane', 'ano_fecha_hecho', 
                        'dia', 'mes', 'numero_semana', 'nombre_mes', 'nombre_dia_semana', 'DepMun', 
                        'NumMes', 'MesAno', 'fecha_hecho']

# Reorganiza las columnas del DataFrame
df_resultados_policia = df_resultados_policia[nuevo_orden_columnas]


# Convierte el DataFrame a una lista de tuplas
lista_de_tuplas = [tuple(row) for row in df_resultados_policia.itertuples(index=True, name=None)]
#lista_de_tuplas = [tuple(row) for row in df_resultados_policia.head(300).itertuples(index=True, name=None)]

#arcpy.AddMessage(lista_de_tuplas[0])

# Función para insertar filas con reintento
def insert_rows_with_retry(cursor, data):
    last_successful_row = None

    try:
        for row in data:
            cursor.insertRow(row)
            #arcpy.AddMessage(insert)
            last_successful_row = row

    except arcpy.ExecuteError as e:
        arcpy.AddMessage(f"Error during insertion: {e}")
        arcpy.AddMessage(arcpy.GetMessages())

    except Exception as e:
        arcpy.AddMessage(f"Unexpected error: {e}")


    return last_successful_row

# Inserta filas con reintento
while len(lista_de_tuplas):
    # Abre un InsertCursor usando un context manager
    with arcpy.da.InsertCursor(target_fc, fieldnames) as cursor:
        try:
            # Intenta insertar todas las filas
            last_successful_row = insert_rows_with_retry(cursor, lista_de_tuplas)
            #arcpy.AddMessage(last_successful_row)

            # Encuentra el índice de la última fila insertada correctamente
            index_last_successful = lista_de_tuplas.index(last_successful_row)

            # Elimina las filas insertadas correctamente de la lista
            lista_de_tuplas = lista_de_tuplas[index_last_successful + 2:]
            #arcpy.AddMessage(len(lista_de_tuplas))

            #if not lista_de_tuplas:
            #    arcpy.AddMessage('NOT LISTA TUPLAS')
            #    break

        except Exception as e:
            arcpy.AddMessage(f"Error during batch insertion: {e}")
            # Rompe el bucle en caso de un error no manejado
            break
"""
arcpy.AddMessage('TERMINO')

session.close()

