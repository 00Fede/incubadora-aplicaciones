-- delete from cc_dev_bancolombia_curated_db.fact_application

-- select count(*) from cc_dev_bancolombia_curated_db.fact_application

-- INSERT INTO cc_dev_bancolombia_curated_db.fact_application
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
coalesce(tao.RedSecurityResourcesCount,0) RedSecurityResourcesCount,
coalesce(tao.YellowSecurityChecksCount,0) YellowSecurityChecksCount,
coalesce(tao.YellowSecurityResourcesCount,0) YellowSecurityResourcesCount,

coalesce(tao.RedFaultToleranceChecksCount,0) RedFaultToleranceChecksCount,
coalesce(tao.RedFaultToleranceResourcesCount,0) RedFaultToleranceResourcesCount,
coalesce(tao.YellowFaultToleranceChecksCount,0) YellowFaultToleranceChecksCount,
coalesce(tao.YellowFaultToleranceResourcesCount,0) YellowFaultToleranceResourcesCount,

coalesce(tao.RedPerformanceChecksCount,0) RedPerformanceChecksCount,
coalesce(tao.RedPerformanceResourcesCount,0) RedPerformanceResourcesCount,
coalesce(tao.YellowPerformanceChecksCount,0) YellowPerformanceChecksCount,
coalesce(tao.YellowPerformanceResourcesCount,0) YellowPerformanceResourcesCount,

coalesce(tao.RedCostOptimizingChecksCount,0) RedCostOptimizingChecksCount,
coalesce(tao.RedCostOptimizingResourcesCount,0) RedCostOptimizingResourcesCount,
coalesce(tao.YellowCostOptimizingChecksCount,0) YellowCostOptimizingChecksCount,
coalesce(tao.YellowCostOptimizingResourcesCount,0) YellowCostOptimizingResourcesCount,

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
count(case when status = 'Red' and category = 'security' then 1 end)  RedSecurityResourcesCount,
count(case when status = 'Yellow' and category = 'security' then 1 end)  YellowSecurityResourcesCount,

count(distinct case when status = 'Red' and category = 'fault_tolerance' then checkid else Null end)  RedFaultToleranceChecksCount,
count(distinct case when status = 'Yellow' and category = 'fault_tolerance' then checkid else Null end)  YellowFaultToleranceChecksCount,
count(case when status = 'Red' and category = 'fault_tolerance' then 1 end)  RedFaultToleranceResourcesCount,
count(case when status = 'Yellow' and category = 'fault_tolerance' then 1 end)  YellowFaultToleranceResourcesCount,

count(distinct case when status = 'Red' and category = 'performance' then checkid else Null end)  RedPerformanceChecksCount,
count(distinct case when status = 'Yellow' and category = 'performance' then checkid else Null end)  YellowPerformanceChecksCount,
count(case when status = 'Red' and category = 'performance' then 1 end)  RedPerformanceResourcesCount,
count(case when status = 'Yellow' and category = 'performance' then 1 end)  YellowPerformanceResourcesCount,

count(distinct case when status = 'Red' and category = 'cost_optimizing' then checkid else Null end)  RedCostOptimizingChecksCount,
count(distinct case when status = 'Yellow' and category = 'cost_optimizing' then checkid else Null end)  YellowCostOptimizingChecksCount,
count(case when status = 'Red' and category = 'cost_optimizing' then 1 end)  RedCostOptimizingResourcesCount,
count(case when status = 'Yellow' and category = 'cost_optimizing' then 1 end)  YellowCostOptimizingResourcesCount

from (
    
        --Check H7IgTzjTYb, COr6dfpM04, COr6dfpM03
        SELECT lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_dev_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_dev_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on 
            fr.volume_id = dr.internal_resource_id 
            and fr.checkid in  ('H7IgTzjTYb', 'COr6dfpM04', 'COr6dfpM03')
            and fr.accountid in (
                                select accountnumber from  "cc_dev_bancolombia_curated_db".dim_account where active = True and status = 'ACTIVE' and environment in                    ('pdn', 'core') 
                                ) 
            and status in ('Red','Yellow')
        where dr.resourcetype = 'AWS::EC2::Volume' 
        and dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231010'
        
        UNION ALL
        
        --Check L4dfs2Q4C5, L4dfs2Q3C2
        SELECT lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_dev_bancolombia_curated_db"."dim_resource" dr 
        inner join (
                    select distinct replace(function_arn, ':'||split(function_arn, ':')[8] ,'') "function arn", category, checkid, status 
                    from "cc_dev_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" 
                    where checkid in ('L4dfs2Q4C5', 'L4dfs2Q3C2')
                    and diacarga='20231010'
                    and accountid in (select accountnumber from  "cc_dev_bancolombia_curated_db".dim_account 
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
        SELECT lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_dev_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_dev_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
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
        and fr.accountid in (select accountnumber from  "cc_dev_bancolombia_curated_db".dim_account 
                            where active = True 
                            and status = 'ACTIVE' 
                            and environment in ('pdn', 'core') 
                            ) 
        and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231010'
        
        UNION ALL
        
        --Checks 1iG5NDGVre, HCP4007jGY
        SELECT lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status
        FROM "cc_dev_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_dev_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on trim(split(fr.security_group_id, '(')[1]) = dr.internal_resource_id 
        and fr.checkid in ('1iG5NDGVre', 'HCP4007jGY') 
        and fr.accountid in (select accountnumber from  "cc_dev_bancolombia_curated_db".dim_account 
                            where active = True 
                            and status = 'ACTIVE' 
                            and environment in ('pdn', 'core') 
                            ) 
        and fr.status in ('Red','Yellow')
        and fr.diacarga='20231010'
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        
         UNION ALL
        
        --Checks BueAdJ7NrP, 796d6f3D83, Pfx0RwqBli, R365s2Qddf, vjafUGJ9H0
        SELECT lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status
        FROM "cc_dev_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_dev_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on fr.bucket_name = dr.internal_resource_id
        and fr.checkid in ('BueAdJ7NrP', '796d6f3D83', 'Pfx0RwqBli', 'R365s2Qddf',
                           'vjafUGJ9H0') 
        and fr.accountid in (select accountnumber from  "cc_dev_bancolombia_curated_db".dim_account 
                            where active = True 
                            and status = 'ACTIVE' 
                            and environment in ('pdn', 'core') 
                            ) 
        and fr.status in ('Red','Yellow')
        and fr.diacarga='20231010'
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        
        UNION ALL
        
        --Checks 8CNsSllI5v, CLOG40CDO8
        -- 
        SELECT lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_dev_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_dev_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on fr.auto_scaling_group_name = dr.resourcename 
        and fr.checkid in ('8CNsSllI5v', 'CLOG40CDO8') 
        and fr.accountid in (select accountnumber from  "cc_dev_bancolombia_curated_db".dim_account 
                            where active = True 
                            and status = 'ACTIVE' 
                            and environment in ('pdn', 'core') 
                            ) 
        and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231010'
        
        UNION ALL
        
        --Checks Wxdfp4B1L1, Wxdfp4B1L4, Wxdfp4B1L3, Wxdfp4B1L2
        -- 
        SELECT lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_dev_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_dev_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on 
            fr.workload_name = dr.resourcename 
            and fr.checkid in ('Wxdfp4B1L1', 'Wxdfp4B1L4', 'Wxdfp4B1L3', 'Wxdfp4B1L2') 
            and fr.accountid in (select accountnumber from  "cc_dev_bancolombia_curated_db".dim_account 
                                where active = True 
                                and status = 'ACTIVE' 
                                and environment in ('pdn', 'core') 
                                ) 
            and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231010'
        
        UNION ALL
        
        --Checks N420c450f2, N420c450f2, N415c450f2, N430c450f2
        -- 
        SELECT lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_dev_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_dev_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on fr.distribution_id = dr.internal_resource_id 
        and fr.checkid in ('N420c450f2', 'N420c450f2', 'N415c450f2', 'N430c450f2') 
        and fr.accountid in (select accountnumber from  "cc_dev_bancolombia_curated_db".dim_account 
                            where active = True 
                            and status = 'ACTIVE' 
                            and environment in ('pdn', 'core') 
                            ) 
        and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231010'
        
        UNION ALL
        
        --Checks f2iK5R6Dep
        -- 
        SELECT lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_dev_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_dev_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on
            fr.db_instance = dr.resourcename
            and fr.checkid in ('f2iK5R6Dep') 
            and fr.accountid in (select accountnumber from  "cc_dev_bancolombia_curated_db".dim_account 
                                where active = True 
                                and status = 'ACTIVE' 
                                and environment in ('pdn', 'core') 
                                ) 
            and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231010'
        
        UNION ALL
        
        --Checks iqdCTZKCUp, 7qGXsKIUw, xdeXZKIUy
        -- 
        SELECT lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_dev_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_dev_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on
            fr.load_balancer_name  = dr.resourcename
            and fr.checkid in ('iqdCTZKCUp', '7qGXsKIUw', 'xdeXZKIUy') 
            and fr.accountid in (select accountnumber from  "cc_dev_bancolombia_curated_db".dim_account 
                                where active = True 
                                and status = 'ACTIVE' 
                                and environment in ('pdn', 'core') 
                                ) 
            and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231010'
        
        UNION ALL
        
        --Checks Qsdfp3A4L4, Qsdfp3A4L2
        -- 
        
        SELECT lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_dev_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_dev_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on
            fr.instance_id = dr.internal_resource_id
            and fr.checkid in ('Qsdfp3A4L4', 'Qsdfp3A4L2') 
            and fr.accountid in (select accountnumber from  "cc_dev_bancolombia_curated_db".dim_account 
                                where active = True 
                                and status = 'ACTIVE' 
                                and environment in ('pdn', 'core') 
                                ) 
            and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231010'
        
        UNION ALL
        
        --Checks c1dfptbg10
        -- 

        SELECT lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_dev_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_dev_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on
            fr.subnet_id = dr.internal_resource_id
            and fr.checkid in ('c1dfptbg10') 
            and fr.accountid in (select accountnumber from  "cc_dev_bancolombia_curated_db".dim_account 
                                where active = True 
                                and status = 'ACTIVE' 
                                and environment in ('pdn', 'core') 
                                ) 
            and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231010'
        
        UNION ALL
        
        --Checks c1t3k8mqv2
        -- 
        SELECT lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_dev_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_dev_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on
            fr.rabbitmq_broker_id = dr.internal_resource_id
            and fr.checkid in ('c1t3k8mqv2') 
            and fr.accountid in (select accountnumber from  "cc_dev_bancolombia_curated_db".dim_account 
                                where active = True 
                                and status = 'ACTIVE' 
                                and environment in ('pdn', 'core') 
                                ) 
            and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231010'
        
        UNION ALL
        
        --Checks c1dfptbg11
        -- 
        SELECT lower(trim(dr.applicationcode)) applicationcode, fr.category, fr.checkid, fr.status 
        FROM "cc_dev_bancolombia_curated_db"."dim_resource" dr 
        inner join "cc_dev_bancolombia_raw_db"."aws_support_trustedadvisor_check_flaggedResources" fr 
        on
            fr.vpc_id = dr.internal_resource_id
            and fr.checkid in ('c1dfptbg11') 
            and fr.accountid in (select accountnumber from  "cc_dev_bancolombia_curated_db".dim_account 
                                where active = True 
                                and status = 'ACTIVE' 
                                and environment in ('pdn', 'core') 
                                ) 
            and status in ('Red','Yellow')
        where  dr.applicationcode is not Null 
        and dr.applicationcode != ''
        and fr.diacarga='20231010'


    )
group by applicationcode
) tao on tao.applicationcode = r.applicationcode