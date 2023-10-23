set from_date ='2023-06-01';
set to_date = '2023-07-31';

//set from_date ='2023-06-01';
//set to_date = '2023-07-31';
  
with 

case_comments_from_comment_table as (
  select cc.id as comment_id,
        cc.parent_id as case_id,
        cc.created_at,
        c.created_at as case_created_at,
        c.closed_at as case_closed_at,
        cc.comment_body,
        cc.is_published,
        case when lower(cc.comment_body) LIKE '%community author%' then 1 else 0 end as customer_comment_boo,
        count(*) over (partition by cc.parent_id order by cc.created_at) as n_comment,
        sum(customer_comment_boo) over (partition by cc.parent_id order by cc.created_at) as n_customer_comment,
        n_comment - n_customer_comment as n_support_comment
  //      n_support_comment - lag(n_support_comment) over (partition by parent_id order by created_at) as n_prev_support_comment


  from "QLIK_ANALYTICS"."CUSTOMER_SUCCESS"."CASE_COMMENT" cc
        left join "QLIK_ANALYTICS"."CUSTOMER_SUCCESS"."CASE" c on cc.parent_id = c.id
  where  case_closed_at >= $from_date
        and case_closed_at < $to_date
        and is_published = True
),

case_initial_comment as (
  select c.id as comment_id, 
         c.id as case_id,
         c.created_at,
         c.created_at as case_created_at,
         c.closed_at,
         c.description as comment_body,
         1 as is_published,
         1 as customer_comment_boo,
         0 as n_customer_comment,
         0 as n_comment,
         0 as n_support_comment
  from "QLIK_ANALYTICS"."CUSTOMER_SUCCESS"."CASE" c 
  where comment_id in (select case_id from case_comments_from_comment_table)
),

case_comments as (
  select * from case_comments_from_comment_table
  union 
  select * from case_initial_comment
),


account_and_case_information as (
  select  c.id as case_id,
          case when c.has_escalated = 'Y' then 1 else 0 end as has_escalated_val,  
          c.created_at as case_created_at,
          c.closed_at as case_closed_at,
          c.subject,
          c.case_number,
          c.product_c as product_category,
          c.priority,
           (case c.priority
                when 'Not Filled' then 0
                when 'Low/Medium' then 1
                when 'Medium/Low' then 1
                when 'Standard' then 1
                when 'P3 - Medium' then 1
                when 'High' then 2
                when 'Urgent' then 3
                else null
                end
           ) as priority_val,
          c.severity_c as severity,
          ai.name as account_name,
          ai.billing_country as account_country,
          ai.billing_country_code as account_country_code,
        
          c.account_id 
  from    "QLIK_ANALYTICS"."CUSTOMER_SUCCESS"."CASE" c
           left join "QLIK_ANALYTICS"."COMMON"."ACCOUNT" ai on c.account_id = ai.id
  where   is_closed = 'TRUE'
),

account_and_case_analyze as (
  select 
          count(aac2.case_id) as n_case,
          sum(aac2.has_escalated_val) as n_escalated,
          n_escalated/n_case as prop_escalated,
          avg(aac2.priority_val) as account_avg_priority_up_to_case,
          avg(aac2.severity) as account_avg_severity_up_to_case
  from account_and_case_information aac1 
  left join account_and_case_information aac2 
    on aac1.case_id = aac2.case_id
    and aac1.case_created_at>=aac2.case_closed_at
  group by aac1.case_id
  
),


//account_and_case_analyze_aggregate_to_comment as (
//select cc.comment_id, cc.case_id, cc.created_at, 
//    Row_number() over (partition by cc.comment_id order by aac.case_created_at desc) as n_seq,
//    aac.n_escalated,
//    aac.n_case,
//    aac.prop_escalated,
//    aac.account_avg_priority_up_to_case,
//    aac.account_avg_severity_up_to_case
//  
//  from case_comments cc left join account_and_case_analyze aac
//    on cc.case_id = aac.case_id
//    and cc.created_at >= aac.case_closed_at //closed_at
//  qualify n_seq = 1
//),

account_base_information_to_comment as (
select cc.comment_id, cc.case_id, cc.created_at, 
       aac.has_escalated_val,
       aac.subject,
       aac.case_number,
       aac.product_category,
       aac.account_id,
       aac.account_name,
       aac.account_country,
       aac.account_country_code
  from case_comments cc left join account_and_case_information aac
  on cc.case_id = aac.case_id
),

associated_jira_aggregate_to_comment as (
select  cc.comment_id, cc.case_id, cc.created_at, aja.created_at as aja_created_at,
        Row_number() over (partition by cc.comment_id order by aja.created_at desc) as n_seq,
        count(aja.id) over (partition by cc.comment_id order by aja.created_at desc) as n_jira_tickets_up_to_comment,
        max(aja.bug_weight_c) over (partition by cc.comment_id order by aja.created_at desc) as bug_weight_max_up_to_comment,
        avg(aja.bug_weight_c) over (partition by cc.comment_id order by aja.created_at desc) as bug_weight_avg_up_to_comment
  from case_comments cc left join "QLIK_ANALYTICS"."CUSTOMER_SUCCESS"."CASE_DEFECT_C" aja
      on cc.case_id = aja.case_c
      and aja.created_at <= cc.created_at
  qualify n_seq = 1
),


severity_priority_history as (
select  case_id, created_at, created_date,
          (case when field = 'Severity__c' then new_Value
           end) as severity,
          (case when field = 'Severity__c' then old_value
           end) as old_severity,
          (case when field = 'Priority' then
            case new_value
                when 'Low/Medium' then 1
                when 'High' then 2
                when 'Urgent' then 3
                else null
                end
           end) as priority,
        (case when field = 'Priority' then
            case old_value
                when 'Low/Medium' then 1
                when 'High' then 2
                when 'Urgent' then 3
                else null
                end
           end) as old_priority,
        (case when old_severity != severity then 1 else 0 end) as severity_diff,
        (case when old_priority != priority then 1 else 0 end) as priority_diff
  from    "QLIK_ANALYTICS"."CUSTOMER_SUCCESS"."CASE_HISTORY" 
  where severity is not NULL 
        or priority is not NULL
),



initial_severity_priority_from_case_table as (
  select case_id,
         severity,
         priority_val as priority,
         case_created_at as created_at
  from account_and_case_information 
//  where aac.id not in (select id from severity_priority_history)
),

initial_severity_priority_from_history_table as (
  select case_id,
         (array_remove(array_agg(old_severity) WITHIN GROUP (ORDER BY created_at asc) , null))[1] as severity,
         (array_remove(array_agg(old_priority) WITHIN GROUP (ORDER BY created_at asc) , null))[1] as priority,
          min(created_at) as first_change_at
  //       created_at
  from severity_priority_history
  group by case_id
),

initial_severity_priority as (
    select sp_case.case_id,
           (case when sp_his.severity is null then sp_case.severity else sp_his.severity end) as severity,
           (case when sp_his.priority is null then sp_case.priority else sp_his.priority end) as priority,
           sp_case.severity as wrong_severity,
           sp_case.priority as wrong_priority,
           (case when sp_case.severity != sp_his.severity then 1 else 0 end) as severity_diff,
           sp_case.created_at
    from initial_severity_priority_from_case_table sp_case 
        left join initial_severity_priority_from_history_table sp_his 
        on sp_his.case_id = sp_case.case_id
),

severity_priority as (
  select case_id, priority, severity, created_at
  from initial_severity_priority 
    union 
  select case_id, priority, severity, created_at
  from severity_priority_history 
),

null_filled_severity_priority as (
  select case_id, created_at,
        coalesce(priority, lag(priority) ignore nulls  over (order by created_at)) as priority  ,
        coalesce(severity, lag(severity) ignore nulls  over (order by created_at)) as severity 
  from severity_priority 
),

severity_priority_aggregate_to_comment as (
select  cc.comment_id, cc.case_id, cc.created_at, spa.created_at as spa_created_at,
        Row_number() over (partition by cc.comment_id order by spa.created_at desc) as n_seq,
        max(severity) over (partition by cc.comment_id order by spa.created_at desc) as severity_max_up_to_comment,
        avg(severity) over (partition by cc.comment_id order by spa.created_at desc) as severity_avg_up_to_comment,
        count(severity) over (partition by cc.comment_id order by spa.created_at desc) as severity_count_up_to_comment,
        severity as severity_latest_up_to_comment,
//        (array_remove(array_agg(severity) over ( partition by cc.comment_id order by spa.created_at desc) , null))[1] as severity_latest_up_to_comment,
        max(priority) over (partition by cc.comment_id order by spa.created_at desc) as priority_max_up_to_comment,
        avg(priority) over (partition by cc.comment_id order by spa.created_at desc) as priority_avg_up_to_comment,
        count(priority) over (partition by cc.comment_id order by spa.created_at desc) as priority_count_up_to_comment,
        priority as priority_latest_up_to_comment
//        (array_remove(array_agg(priority) over ( partition by cc.comment_id order by spa.created_at desc) , null))[1] as priority_latest_up_to_comment
  from case_comments cc left join null_filled_severity_priority spa
      on cc.case_id = spa.case_id 
      and spa.created_at <= cc.created_at
  qualify n_seq = 1
),





output as (    
select cc.comment_id,
        cc.case_id,
        cc.created_at,
        cc.comment_body,
        cc.customer_comment_boo,
        cc.n_comment,
        cc.n_customer_comment,
        cc.n_support_comment,
        cc.case_closed_at,
        cc.case_created_at,
        aac.has_escalated_val,
        aac.account_id,
        aac.case_number,
        aac.subject,
        aac.product_category,
        aac.account_name,
        aac.account_country,
        aac.account_country_code,
        aja.created_at as associated_jira_aggregate_created_at,
        aja.n_jira_tickets_up_to_comment,
        aja.bug_weight_max_up_to_comment,
        aja.bug_weight_avg_up_to_comment,
        spa.created_at as serverity_priority_aggregate_created_at,
        spa.severity_max_up_to_comment,
        spa.severity_avg_up_to_comment,
        spa.severity_count_up_to_comment,
        spa.severity_latest_up_to_comment,
        spa.priority_max_up_to_comment,
        spa.priority_avg_up_to_comment,
        spa.priority_count_up_to_comment,
        spa.priority_latest_up_to_comment
     
        
from case_comments cc
    left join account_base_information_to_comment aac on cc.comment_id = aac.comment_id
    left join associated_jira_aggregate_to_comment aja on cc.comment_id = aja.comment_id
    left join severity_priority_aggregate_to_comment spa on cc.comment_id = spa.comment_id
  
where has_escalated_val is not null
//  and account_country in (
//        'United States of America (the)',
//      'Germany',
//      'Brazil',
//      'United Kingdom of Great Britain and Northern Ireland (the)',
//      'Netherlands (the)',
//      'Sweden',
//      'India',
//      'Australia',
//      'Canada',
//      'South Africa',
//      'Denmark'
//      )

),

final_table as ( SELECT 1 + 1 AS result
)

select *  from output
//select case_number from "QLIK_ANALYTICS"."CUSTOMER_SUCCESS"."CASE"





//group by account_country


//select count(*), count(distinct case_id), count(first_severity) from test
//select * from test
//where case_id = '5003z00002aQvnXAAS'
//order by created_at
//where n_seq = 1

//select count(*) from severity_priority_aggregate_to_comment

//select * from severity_priority_aggregate_to_comment
//where case_id = '5003z00002aQvnXAAS'
//order by created_at

//select * from test
//where n_seq = 1 
//and first_severity is null

//select * from test

//select  * from test 
//where n_comment = (select max(n_comment) from test)

//select * 
//from case_comments
//where case_id = '5003z00002bHesRAAS'
//order by created_at


//select max(n_comment) from test

//select count(*), count(severity_avg_up_to_comment), count(severity_latest_up_to_comment) from severity_priority_aggregate_to_comment




