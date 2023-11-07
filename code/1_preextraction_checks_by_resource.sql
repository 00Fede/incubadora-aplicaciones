-- Tabla Temporal donde se tendrá el resultado de la evaluación de todos los checks de Arquitectura (generados automaticamente por AWS Trusted Advisor) 
-- asociados a recursos de infraestructura de Aplicaciones desplegadas en la nube AWS de Bancolombia
CREATE TABLE "cc_pdn_bancolombia_curated_db"."tmp_checks_by_resource" AS 
        --Check H7IgTzjTYb, COr6dfpM04, COr6dfpM03
        SELECT dr.resource_id , lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_pdn_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_pdn_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on 
            fr.volume_id = dr.internal_resource_id 
            and fr.checkid in  ('H7IgTzjTYb', 'COr6dfpM04', 'COr6dfpM03')
            and fr.accountid in (
                                select accountnumber from  "cc_pdn_bancolombia_curated_db".dim_account where active = True and status = 'ACTIVE' and environment in                    ('pdn', 'core') 
                                ) 
            and status in ('Red','Yellow')
        where dr.resourcetype = 'AWS::EC2::Volume' 
        and dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231105'
        
        UNION ALL
        
        --Check L4dfs2Q4C5, L4dfs2Q3C2
        SELECT dr.resource_id , lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_pdn_bancolombia_curated_db"."dim_resource" dr 
        inner join (
                    select distinct replace(function_arn, ':'||split(function_arn, ':')[8] ,'') "function arn", category, checkid, status 
                    from "cc_pdn_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" 
                    where checkid in ('L4dfs2Q4C5', 'L4dfs2Q3C2')
                    and diacarga='20231105'
                    and accountid in (select accountnumber from  "cc_pdn_bancolombia_curated_db".dim_account 
                                      where active = True and status = 'ACTIVE' 
                                      and environment in ('pdn', 'core') 
                                    )  
                    and status in ('Red','Yellow') 
                    ) fr 
        on 
        fr."function arn" = dr.internal_id
        where dr.applicationcode is not Null and dr.applicationcode != ''
        
        
        UNION ALL
        
        --Checks Hs4Ma3G124, Hs4Ma3G242, Hs4Ma3G178, Hs4Ma3G135, Hs4Ma3G173, Hs4Ma3G118, Hs4Ma3G123, Hs4Ma3G238, Hs4Ma3G171, Hs4Ma3G200
        -- Hs4Ma3G169, Hs4Ma3G130, Hs4Ma3G117, Hs4Ma3G194, Hs4Ma3G185, Hs4Ma3G137, Hs4Ma3G277, Hs4Ma3G247, Hs4Ma3G214, Hs4Ma3G243
        -- Hs4Ma3G172, Hs4Ma3G133, Hs4Ma3G248, Hs4Ma3G155, Hs4Ma3G276, Hs4Ma3G168, Hs4Ma3G179, Hs4Ma3G262, Hs4Ma3G235, Hs4Ma3G263
        -- Hs4Ma3G245, Hs4Ma3G184, Hs4Ma3G206, Hs4Ma3G270, Hs4Ma3G127, Hs4Ma3G210, Hs4Ma3G153, Hs4Ma3G136, Hs4Ma3G212, Hs4Ma3G230
        -- Hs4Ma3G278, Hs4Ma3G252, Hs4Ma3G134, Hs4Ma3G250, Hs4Ma3G293, Hs4Ma3G211, Hs4Ma3G164, Hs4Ma3G233, Hs4Ma3G280, Hs4Ma3G139
        -- Hs4Ma3G256, Hs4Ma3G125, Hs4Ma3G129, Hs4Ma3G193, Hs4Ma3G251, Hs4Ma3G110
        SELECT dr.resource_id , lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_pdn_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_pdn_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on fr.resource = dr.internal_id 
        and fr.checkid in ('Hs4Ma3G124', 'Hs4Ma3G242', 'Hs4Ma3G178', 'Hs4Ma3G117',
                           'Hs4Ma3G135', 'Hs4Ma3G173', 'Hs4Ma3G118', 'Hs4Ma3G194',
                           'Hs4Ma3G123', 'Hs4Ma3G238', 'Hs4Ma3G171', 'Hs4Ma3G137',
                           'Hs4Ma3G200', 'Hs4Ma3G169', 'Hs4Ma3G130', 'Hs4Ma3G185',
                           'Hs4Ma3G277', 'Hs4Ma3G247', 'Hs4Ma3G214', 'Hs4Ma3G243',
                           'Hs4Ma3G172', 'Hs4Ma3G133', 'Hs4Ma3G248', 'Hs4Ma3G155',
                           'Hs4Ma3G276', 'Hs4Ma3G168', 'Hs4Ma3G179', 'Hs4Ma3G262',
                           'Hs4Ma3G235', 'Hs4Ma3G263', 'Hs4Ma3G245', 'Hs4Ma3G184',
                           'Hs4Ma3G206', 'Hs4Ma3G270', 'Hs4Ma3G127', 'Hs4Ma3G210',
                           'Hs4Ma3G153', 'Hs4Ma3G136', 'Hs4Ma3G212', 'Hs4Ma3G230',
                           'Hs4Ma3G278', 'Hs4Ma3G252', 'Hs4Ma3G199', 'Hs4Ma3G134',
                           'Hs4Ma3G250', 'Hs4Ma3G293', 'Hs4Ma3G211', 'Hs4Ma3G164',
                           'Hs4Ma3G233', 'Hs4Ma3G280', 'Hs4Ma3G139', 'Hs4Ma3G256',
                           'Hs4Ma3G125', 'Hs4Ma3G129', 'Hs4Ma3G193', 'Hs4Ma3G251',
                           'Hs4Ma3G110') 
        and fr.accountid in (select accountnumber from  "cc_pdn_bancolombia_curated_db".dim_account 
                            where active = True 
                            and status = 'ACTIVE' 
                            and environment in ('pdn', 'core') 
                            ) 
        and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231105'
        
        UNION ALL
        
        --Checks 1iG5NDGVre, HCP4007jGY
        SELECT dr.resource_id , lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status
        FROM "cc_pdn_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_pdn_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on trim(split(fr.security_group_id, '(')[1]) = dr.internal_resource_id 
        and fr.checkid in ('1iG5NDGVre', 'HCP4007jGY') 
        and fr.accountid in (select accountnumber from  "cc_pdn_bancolombia_curated_db".dim_account 
                            where active = True 
                            and status = 'ACTIVE' 
                            and environment in ('pdn', 'core') 
                            ) 
        and fr.status in ('Red','Yellow')
        and fr.diacarga='20231105'
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        
         UNION ALL
        
        --Checks BueAdJ7NrP, 796d6f3D83, Pfx0RwqBli, R365s2Qddf, vjafUGJ9H0
        SELECT dr.resource_id , lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status
        FROM "cc_pdn_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_pdn_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on fr.bucket_name = dr.internal_resource_id
        and fr.checkid in ('BueAdJ7NrP', '796d6f3D83', 'Pfx0RwqBli', 'R365s2Qddf',
                           'vjafUGJ9H0') 
        and fr.accountid in (select accountnumber from  "cc_pdn_bancolombia_curated_db".dim_account 
                            where active = True 
                            and status = 'ACTIVE' 
                            and environment in ('pdn', 'core') 
                            ) 
        and fr.status in ('Red','Yellow')
        and fr.diacarga='20231105'
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        
        UNION ALL
        
        --Checks 8CNsSllI5v, CLOG40CDO8
        -- 
        SELECT dr.resource_id , lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_pdn_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_pdn_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on fr.auto_scaling_group_name = dr.resourcename 
        and fr.checkid in ('8CNsSllI5v', 'CLOG40CDO8') 
        and fr.accountid in (select accountnumber from  "cc_pdn_bancolombia_curated_db".dim_account 
                            where active = True 
                            and status = 'ACTIVE' 
                            and environment in ('pdn', 'core') 
                            ) 
        and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231105'
        
        UNION ALL
        
        --Checks Wxdfp4B1L1, Wxdfp4B1L4, Wxdfp4B1L3, Wxdfp4B1L2
        -- 
        SELECT dr.resource_id , lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_pdn_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_pdn_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on 
            fr.workload_name = dr.resourcename 
            and fr.checkid in ('Wxdfp4B1L1', 'Wxdfp4B1L4', 'Wxdfp4B1L3', 'Wxdfp4B1L2') 
            and fr.accountid in (select accountnumber from  "cc_pdn_bancolombia_curated_db".dim_account 
                                where active = True 
                                and status = 'ACTIVE' 
                                and environment in ('pdn', 'core') 
                                ) 
            and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231105'
        
        UNION ALL
        
        --Checks N420c450f2, N420c450f2, N415c450f2, N430c450f2
        -- 
        SELECT dr.resource_id , lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_pdn_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_pdn_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on fr.distribution_id = dr.internal_resource_id 
        and fr.checkid in ('N420c450f2', 'N420c450f2', 'N415c450f2', 'N430c450f2') 
        and fr.accountid in (select accountnumber from  "cc_pdn_bancolombia_curated_db".dim_account 
                            where active = True 
                            and status = 'ACTIVE' 
                            and environment in ('pdn', 'core') 
                            ) 
        and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231105'
        
        UNION ALL
        
        --Checks f2iK5R6Dep
        -- 
        SELECT dr.resource_id , lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_pdn_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_pdn_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on
            fr.db_instance = dr.resourcename
            and fr.checkid in ('f2iK5R6Dep') 
            and fr.accountid in (select accountnumber from  "cc_pdn_bancolombia_curated_db".dim_account 
                                where active = True 
                                and status = 'ACTIVE' 
                                and environment in ('pdn', 'core') 
                                ) 
            and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231105'
        
        UNION ALL
        
        --Checks iqdCTZKCUp, 7qGXsKIUw, xdeXZKIUy
        -- 
        SELECT dr.resource_id , lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_pdn_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_pdn_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on
            fr.load_balancer_name  = dr.resourcename
            and fr.checkid in ('iqdCTZKCUp', '7qGXsKIUw', 'xdeXZKIUy') 
            and fr.accountid in (select accountnumber from  "cc_pdn_bancolombia_curated_db".dim_account 
                                where active = True 
                                and status = 'ACTIVE' 
                                and environment in ('pdn', 'core') 
                                ) 
            and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231105'
        
        UNION ALL
        
        --Checks Qsdfp3A4L4, Qsdfp3A4L2
        -- 
        
        SELECT dr.resource_id , lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_pdn_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_pdn_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on
            fr.instance_id = dr.internal_resource_id
            and fr.checkid in ('Qsdfp3A4L4', 'Qsdfp3A4L2') 
            and fr.accountid in (select accountnumber from  "cc_pdn_bancolombia_curated_db".dim_account 
                                where active = True 
                                and status = 'ACTIVE' 
                                and environment in ('pdn', 'core') 
                                ) 
            and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231105'
        
        UNION ALL
        
        --Checks c1dfptbg10
        -- 

        SELECT dr.resource_id , lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_pdn_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_pdn_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on
            fr.subnet_id = dr.internal_resource_id
            and fr.checkid in ('c1dfptbg10') 
            and fr.accountid in (select accountnumber from  "cc_pdn_bancolombia_curated_db".dim_account 
                                where active = True 
                                and status = 'ACTIVE' 
                                and environment in ('pdn', 'core') 
                                ) 
            and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231105'
        
        UNION ALL
        
        --Checks c1t3k8mqv2
        -- 
        SELECT dr.resource_id , lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_pdn_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_pdn_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on
            fr.rabbitmq_broker_id = dr.internal_resource_id
            and fr.checkid in ('c1t3k8mqv2') 
            and fr.accountid in (select accountnumber from  "cc_pdn_bancolombia_curated_db".dim_account 
                                where active = True 
                                and status = 'ACTIVE' 
                                and environment in ('pdn', 'core') 
                                ) 
            and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231105'
        
        UNION ALL
        
        --Checks c1dfptbg11
        -- 
        SELECT dr.resource_id , lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_pdn_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_pdn_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on
            fr.vpc_id = dr.internal_resource_id
            and fr.checkid in ('c1dfptbg11') 
            and fr.accountid in (select accountnumber from  "cc_pdn_bancolombia_curated_db".dim_account 
                                where active = True 
                                and status = 'ACTIVE' 
                                and environment in ('pdn', 'core') 
                                ) 
            and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231105'




CREATE TABLE `ta_checks_by_resource`(
  resource_id bigint, 
  applicationcode string, 
  category string, 
  checkid string, 
  status string)
PARTITIONED BY (category)
LOCATION 's3://cld0014001-cloudcenter-pdn-bancolombia-s3-curated/ta_checks_by_resource'
TBLPROPERTIES (
  'table_type'='ICEBERG',
  'format'='parquet',
  'write_target_data_file_size_bytes'='536870912' 
)
;

insert into ta_checks_by_resource 
select * from tmp_checks_by_resource 


