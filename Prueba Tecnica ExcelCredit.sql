create or replace function is_date(s varchar) returns boolean as $$
begin
  perform TO_DATE(s,'DD/MM/YYYY');
  return true;
exception when others then
  return false;
end;
$$ language plpgsql;

CREATE TABLE IF NOT EXISTS  Tratados_intenacionales(
"Nombre del Tratado" character varying(100000),
"Bilateral" character varying(10000),
"Lugar de Adopcion" character varying(10000),
"Fecha de Adopcion" character varying(10000),
"Estados-Organismos" character varying(10000),
"Temas" character varying(10000),
"Naturaleza del Tratado" character varying(10000),
"Depositario" character varying(10000),
"Suscribio Por Colombia" character varying(10000),
"Vigente" character varying(10000),
"Fecha Ley Aprobatoria" character varying(10000),
"Numero Ley Aprobatoria" character varying(10000),
"Sentencia Fecha Ley" character varying(10000),
"Sentencia Numero" character varying(10000),
"Decreto Fecha Diario Oficial" character varying(10000),
"Decreto Numero Diario Oficial" character varying(10000)
);

copy Tratados_intenacionales from program 'curl "https://raw.githubusercontent.com/sebas317/testDataEnginner/main/Tratados_internacionales_de_Colombia.csv"' delimiter ',' csv header;

select count(*) from Tratados_intenacionales

create unlogged table restCountries (doc json);

COPY restCountries FROM program 'curl "https://restcountries.com/v2/all"'
  
create table real_json as select value::jsonb from restcountries join lateral json_array_elements(doc::json) on true;

CREATE TABLE IF NOT EXISTS Tabla_Union(
"Nombre del Tratado" character varying(100000),
"Bilateral" bool,
"Lugar de Adopción" character varying(10000),
"Fecha de Adopción" date,
"Estados-Organismos" character varying(10000),
"Temas" character varying(10000),
"Naturaleza del Tratado" character varying(10000),
"Depositario" character varying(10000),
"Suscribió Por Colombia" character varying(10000),
"Vigente" bool,
"Fecha Ley Aprobatoria" date,
"Número Ley Aprobatoria" character varying(10000),
"Sentencia Fecha Ley" date,
"Sentencia Número" character varying(10000),
"Decreto Fecha Diario Oficial" date,
"Decreto Número Diario Oficial" character varying(10000),
"País del tratado" character varying(10000),
"Código de llamadas" character varying(10000),
"Capital" character varying(10000),
"Región" character varying(10000),
"Subregión" character varying(10000),
"Población" character varying(10000),
"Área" int4,
"Zona horaria" character varying(10000),
"Monedas" character varying(10000), 
"Idiomas" character varying(10000), 
"Cantidad fronteras" int4,
"Diferencia horas zona horaria" decimal
)


insert into tabla_union select *
FROM(
select
case when ti."Nombre del Tratado"='(NO REGISTRA)' then NULL else ti."Nombre del Tratado" END,
case when ti."Bilateral" ='NO' then false else true END,
case when ti."Lugar de Adopcion" ='(NO REGISTRA)' then NULL else ti."Lugar de Adopcion" END,
case when ti."Fecha de Adopcion" ='(NO REGISTRA)' OR is_date(ti."Fecha de Adopcion")=false then NULL else to_date (ti."Fecha de Adopcion",'DD/MM/YYYY') END,
case when ti."Estados-Organismos" ='(NO REGISTRA)' then NULL else ti."Estados-Organismos" END,
case when ti."Temas" ='(NO REGISTRA)' then NULL else ti."Temas" END,
case when ti."Naturaleza del Tratado" ='(NO REGISTRA)' then NULL else ti."Naturaleza del Tratado" END,
case when ti."Depositario" ='(NO REGISTRA)' then NULL else ti."Depositario" END,
case when ti."Suscribio Por Colombia" ='(NO REGISTRA)' then NULL else ti."Suscribio Por Colombia" END,
case when ti."Vigente" ='NO' then false else true END,
case when ti."Fecha Ley Aprobatoria"  ='(NO REGISTRA)' OR is_date(ti."Fecha Ley Aprobatoria") =false then NULL else TO_DATE(ti."Fecha Ley Aprobatoria",'DD/MM/YYYY') END,
case when ti."Numero Ley Aprobatoria" ='(NO REGISTRA)' then NULL else ti."Numero Ley Aprobatoria" END,
case when ti."Sentencia Fecha Ley" ='(NO REGISTRA)' OR is_date(ti."Sentencia Fecha Ley")=false then NULL else TO_DATE(ti."Sentencia Fecha Ley",'DD/MM/YYYY') END,
case when ti."Sentencia Numero" ='(NO REGISTRA)' then NULL else ti."Sentencia Numero" END,
case when ti."Decreto Fecha Diario Oficial" ='(NO REGISTRA)' OR is_date(ti."Decreto Fecha Diario Oficial")=false then NULL else TO_DATE(ti."Decreto Fecha Diario Oficial" ,'DD/MM/YYYY') END,
case when ti."Decreto Numero Diario Oficial" ='(NO REGISTRA)' then NULL else ti."Decreto Numero Diario Oficial" END,
upper(re.value::json->'translations'->>'es') as pais, replace(cast(re.value::json->'callingCodes'->0 as varchar(6)),'"','') as codigo,
upper(re.value::json->>'capital') as CAPITAL,
upper(re.value::json->>'region') as REGION,
upper(re.value::json->>'subregion') as SUBREGION,
re.value::json->>'population' as population,
NULLIF(re.value::json->>'area', '')::decimal::integer,
replace(cast(re.value::json->'timezones'->0 as varchar(11)),'"',''),
replace(replace(replace(replace(replace(cast(re.value::json->'currencies' as varchar(255)),'"',''),'{',''),'}',''),'[',''),']',''),
replace(replace(replace(replace(replace(cast(re.value::json->'languages' as varchar(255)),'"',''),'{',''),'}',''),'[',''),']',''),
json_array_length(re.value::json->'borders'),
case when LENGTH(replace(substring(cast(re.value::json->'timezones'->0 as varchar(10)),5,3),'"',''))=3 then to_number(substring(cast(re.value::json->'timezones'->0 as varchar(10)),5,3),'S99')-(-5) else 0 END
from tratados_intenacionales ti inner join real_json re
on upper(re.value::json->'translations'->>'es')=ti."Estados-Organismos"
--where asinteger(re.value::json->>'area')=0
) as T2


select tu."País del tratado" as "pais",  (select count(u1.*) as vigentes from tabla_union u1 where tu."País del tratado"=u1."País del tratado" and u1."Vigente" = true ) as "cantidad de tratados vigentes",
(select count(u1.*) as vigentes from tabla_union u1 where tu."País del tratado"=u1."País del tratado" and u1."Vigente" = false ) as "cantidad de tratados no vigentes",
min(tu."Fecha de Adopción") as "fecha adopción primer tratado",
(DATE_PART('year', current_date) - DATE_PART('year', min(tu."Fecha de Adopción"))) * 12 + (DATE_PART('month', current_date) - DATE_PART('month', min(tu."Fecha de Adopción")))
as "diferencia en meses desde el primer acuerdo a la fecha actual"
into agrupacion_por_pais
from tabla_union as tu  
where tu."Fecha de Adopción" is not null
group by tu."País del tratado";
