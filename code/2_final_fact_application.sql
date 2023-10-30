/* Tabla Final que contiene el dataset con el inventario de todas las Aplicaciones desplegadas en la nube AWS  de Bancolombia  */
/* Corresponde a datos tomadas de las fuentes AWS Trusted Advisor y AWS Config del 17 de octubre de 2023*/

/* Consulta de Creación de la tabla relacionada que contedrá el dataset*/

CREATE TABLE cc_dev_bancolombia_curated_db.fact_application(
application_id int,
organization_id int,
provider_id int,
snapshot_date_id bigint,
applicationcode string,
FullResourcesCount bigint,
DevResourcesCount bigint,
QaResourcesCount bigint,
PdnResourcesCount bigint,
PdnResourcesSize decimal(5,3),
FullAccountsCount bigint,
DevAccountsCount bigint,
QaAccountsCount bigint,
PdnAccountsCount bigint,
RedSecurityChecksCount bigint,
RedSecurityResourcesCount bigint,
YellowSecurityChecksCount bigint,
YellowSecurityResourcesCount bigint,
RedFaultToleranceChecksCount bigint,
RedFaultToleranceResourcesCount bigint,
YellowFaultToleranceChecksCount bigint,
YellowFaultToleranceResourcesCount bigint,
RedPerformanceChecksCount bigint,
RedPerformanceResourcesCount bigint,
YellowPerformanceChecksCount bigint,
YellowPerformanceResourcesCount bigint,
RedCostOptimizingChecksCount bigint,
RedCostOptimizingResourcesCount bigint,
YellowCostOptimizingChecksCount bigint,
YellowCostOptimizingResourcesCount bigint,
RedSecurityDensity decimal(4,3),
YellowSecurityDensity decimal(4,3),
RedFaultToleranceDensity decimal(4,3),
YellowFaultToleranceDensity decimal(4,3),
RedPerformanceDensity decimal(4,3),
YellowPerformanceDensity decimal(4,3),
RedCostOptimizingDensity decimal(4,3),
YellowCostOptimizingDensity decimal(4,3),
ppn_tm timestamp,
PdnResourcesSize decimal(5,3),
Clasification string
  )
PARTITIONED BY (organization_id, provider_id, snapshot_date_id)
LOCATION 's3://cld0014001-cloudcenter-dev-bancolombia-s3-curated/fact_application'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_target_data_file_size_bytes'='536870912' 
)
;




/* Query de llenado de la tabla*/
--
INSERT INTO cc_dev_bancolombia_curated_db.fact_application 
SELECT  
p.application_id,
o.organization_id, 
dp.provider_id, 
Cast(date_format(current_date, '%Y%m%d') as bigint) as snapshot_date_id, 
'app' || lpad(cast(p.application_id as varchar), 5, '0') applicationcode,
r.countFull FullResourcesCount,
r.countDev DevResourcesCount,
r.countQa QaResourcesCount,
r.countPdn PdnResourcesCount,
r.FullAccountsCount,
r.DevAccountsCount,
r.QaAccountsCount,
r.PdnAccountsCount,

coalesce(tao.RedSecurityChecksCount,0) RedSecurityChecksCount,
coalesce(tao2.RedSecurityResourcesCount,0) RedSecurityResourcesCount,
coalesce(tao.YellowSecurityChecksCount,0) YellowSecurityChecksCount,
coalesce(tao2.YellowSecurityResourcesCount,0) YellowSecurityResourcesCount,

coalesce(tao.RedFaultToleranceChecksCount,0) RedFaultToleranceChecksCount,
coalesce(tao2.RedFaultToleranceResourcesCount,0) RedFaultToleranceResourcesCount,
coalesce(tao.YellowFaultToleranceChecksCount,0) YellowFaultToleranceChecksCount,
coalesce(tao2.YellowFaultToleranceResourcesCount,0) YellowFaultToleranceResourcesCount,

coalesce(tao.RedPerformanceChecksCount,0) RedPerformanceChecksCount,
coalesce(tao2.RedPerformanceResourcesCount,0) RedPerformanceResourcesCount,
coalesce(tao.YellowPerformanceChecksCount,0) YellowPerformanceChecksCount,
coalesce(tao2.YellowPerformanceResourcesCount,0) YellowPerformanceResourcesCount,

coalesce(tao.RedCostOptimizingChecksCount,0) RedCostOptimizingChecksCount,
coalesce(tao2.RedCostOptimizingResourcesCount,0) RedCostOptimizingResourcesCount,
coalesce(tao.YellowCostOptimizingChecksCount,0) YellowCostOptimizingChecksCount,
coalesce(tao2.YellowCostOptimizingResourcesCount,0) YellowCostOptimizingResourcesCount,

0,0,0,0,0,0,0,0,

cast(current_timestamp as timestamp) as ppn_tm
FROM (
select lower(trim(d.applicationcode)) applicationcode, 
count(1) countFull, count(case when a.environment in ('pdn', 'core') then 1 end ) countPdn, count(case when a.environment in ('qa') then 1 end ) countQa, count(case when a.environment in ('dev') then 1 end ) countDev,
count(distinct a.accountnumber) FullAccountsCount,
count( distinct case when a.environment in ('dev') then a.accountnumber else Null end  ) DevAccountsCount,
count( distinct case when a.environment in ('qa') then a.accountnumber else Null end  ) QaAccountsCount,
count( distinct case when a.environment in ('pdn', 'core') then a.accountnumber else Null end  ) PdnAccountsCount
from cc_dev_bancolombia_curated_db.dim_resource d 
inner join cc_dev_bancolombia_curated_db.dim_account a on d.accountid = a.accountnumber and a.active = True and a.status = 'ACTIVE' and a.environment in ('pdn', 'core', 'qa', 'dev')
where d.active = True and d.applicationcode is not null and d.applicationcode != '' 
group by lower(trim(d.applicationcode)) 
) r
inner join cc_dev_bancolombia_curated_db.dim_application p on  r.applicationcode = p.applicationcode
inner join cc_dev_bancolombia_curated_db.dim_organization o on  o.name = 'bancolombia'
inner join cc_dev_bancolombia_curated_db.dim_provider dp on  dp.shortname = 'aws'

left outer join (
    select
    applicationcode, 

    count(distinct case when status = 'Red' and category = 'security' then checkid else Null end)  RedSecurityChecksCount,
    count(distinct case when status = 'Yellow' and category = 'security' then checkid else Null end)  YellowSecurityChecksCount,
    count(distinct case when status = 'Red' and category = 'fault_tolerance' then checkid else Null end)  RedFaultToleranceChecksCount,
    count(distinct case when status = 'Yellow' and category = 'fault_tolerance' then checkid else Null end)  YellowFaultToleranceChecksCount,
    count(distinct case when status = 'Red' and category = 'performance' then checkid else Null end)  RedPerformanceChecksCount,
    count(distinct case when status = 'Yellow' and category = 'performance' then checkid else Null end)  YellowPerformanceChecksCount,
    count(distinct case when status = 'Red' and category = 'cost_optimizing' then checkid else Null end)  RedCostOptimizingChecksCount,
    count(distinct case when status = 'Yellow' and category = 'cost_optimizing' then checkid else Null end)  YellowCostOptimizingChecksCount

    FROM "cc_dev_bancolombia_curated_db"."tmp_checks_by_resource" group by applicationcode
) tao on tao.applicationcode = r.applicationcode

left outer join (
    select
    applicationcode, 
    count(distinct case when status = 'Red' and category = 'security' then resource_id else Null end)  RedSecurityResourcesCount,
    count(distinct case when status = 'Yellow' and category = 'security' then resource_id else Null end)  YellowSecurityResourcesCount,
    count(distinct case when status = 'Red' and category = 'fault_tolerance' then resource_id else Null end)  RedFaultToleranceResourcesCount,
    count(distinct case when status = 'Yellow' and category = 'fault_tolerance' then resource_id else Null end)  YellowFaultToleranceResourcesCount,
    count(distinct case when status = 'Red' and category = 'performance' then resource_id else Null end)  RedPerformanceResourcesCount,
    count(distinct case when status = 'Yellow' and category = 'performance' then resource_id else Null end)  YellowPerformanceResourcesCount,
    count(distinct case when status = 'Red' and category = 'cost_optimizing' then resource_id else Null end)  RedCostOptimizingResourcesCount,
    count(distinct case when status = 'Yellow' and category = 'cost_optimizing' then resource_id else Null end)  YellowCostOptimizingResourcesCount

    FROM "cc_dev_bancolombia_curated_db"."tmp_checks_by_resource" group by applicationcode
) tao2 on tao2.applicationcode = r.applicationcode    





/* Consultas de actualización para calcular algunas caracteristicas adicionales de las aplicacinoes*/
UPDATE cc_dev_bancolombia_curated_db.fact_application set redsecuritydensity = redsecurityresourcescount  / cast(pdnresourcescount as decimal(10,3) ) where pdnresourcescount > 0;
UPDATE cc_dev_bancolombia_curated_db.fact_application set yellowsecuritydensity = yellowsecurityresourcescount  / cast(pdnresourcescount as decimal(10,3)) where pdnresourcescount > 0;

UPDATE cc_dev_bancolombia_curated_db.fact_application set redfaulttolerancedensity = redfaulttoleranceresourcescount  / cast(pdnresourcescount as decimal(10,3) ) where pdnresourcescount > 0;
UPDATE cc_dev_bancolombia_curated_db.fact_application set yellowfaulttolerancedensity = yellowfaulttoleranceresourcescount  / cast(pdnresourcescount as decimal(10,3)) where pdnresourcescount > 0;

UPDATE cc_dev_bancolombia_curated_db.fact_application set redperformancedensity = redperformanceresourcescount  / cast(pdnresourcescount as decimal(10,3) ) where pdnresourcescount > 0;
UPDATE cc_dev_bancolombia_curated_db.fact_application set yellowperformancedensity = yellowperformanceresourcescount  / cast(pdnresourcescount as decimal(10,3)) where pdnresourcescount > 0;

UPDATE cc_dev_bancolombia_curated_db.fact_application set redcostoptimizingdensity = redcostoptimizingresourcescount  / cast(pdnresourcescount as decimal(10,3) ) where pdnresourcescount > 0;
UPDATE cc_dev_bancolombia_curated_db.fact_application set yellowcostoptimizingdensity = yellowcostoptimizingresourcescount  / cast(pdnresourcescount as decimal(10,3)) where pdnresourcescount > 0;

UPDATE cc_dev_bancolombia_curated_db.fact_application set pdnresourcessize = log10(pdnresourcescount) where pdnresourcescount > 0;

--green ..  saludable .. no requiere atención ... (sin checks en yellow red -- o con poco tamaño y baja densidad de checks en yellow ) ....  
update "cc_dev_bancolombia_curated_db"."fact_application" set clasification = 'Green' where pdnresourcescount > 0  and (redcostoptimizingdensity=0 and redperformancedensity=0 and redfaulttolerancedensity = 0 and redsecuritydensity = 0) and (yellowcostoptimizingdensity<0.001 and yellowperformancedensity<0.001 and yellowfaulttolerancedensity <0.001 and yellowsecuritydensity <0.001) and pdnresourcessize < 0.5;

--yellow ... requiere atención moderada ... (con cualquier densidad en yellow y muy bajas en red y poco tamaño o mediano)
update "cc_dev_bancolombia_curated_db"."fact_application" set clasification = 'Yellow' where pdnresourcescount > 0  and (redcostoptimizingdensity between 0.1 and 0.5  or redperformancedensity between 0.1 and 0.5 or redfaulttolerancedensity between 0.1 and 0.5 or redsecuritydensity between 0.1 and 0.5) and pdnresourcessize < 1;

--red ... en estado crítico y para atención inmediata  (densidades en rojo mas elevadas en cualquier tamaño de aplicación)
update  "cc_dev_bancolombia_curated_db"."fact_application" set clasification = 'Red' where pdnresourcescount > 0  and (redcostoptimizingdensity >= 0.5 or redperformancedensity >= 0.5 or redfaulttolerancedensity >= 0.5 or redsecuritydensity >= 0.5) ;

