set from_date ='2023-01-01';
set to_date = '2023-05-31';
  
with 

case_comments as (
  select cc.id,
        cc.parent_id,
        cc.created_at,
        cc.created_date,
        cc.comment_body,
        cc.is_published,
        case when lower(cc.comment_body) LIKE '%community author%' then 1 else 0 end as customer_comment_boo,
        count(*) over (partition by cc.parent_id order by cc.created_at) as n_comment,
        sum(customer_comment_boo) over (partition by cc.parent_id order by cc.created_at) as n_customer_comment,
        n_comment - n_customer_comment as n_support_comment
  //      n_support_comment - lag(n_support_comment) over (partition by parent_id order by created_at) as n_prev_support_comment


  from "QLIK_ANALYTICS"."CUSTOMER_SUCCESS"."CASE_COMMENT" cc
  where  CREATED_DATE >= $from_date
        and CREATED_DATE < $to_date
        and is_published = True
),

account_and_case_analyze as (
  select  id,
          count(id) over (partition by account_id order by created_at) as n_case,
  
  
          case when has_escalated = 'Y' then 1 else 0 end as has_escalated_val,    
 
          sum(has_escalated_val) over (partition by account_id order by created_at) as n_escalated,
          n_escalated/n_case as prop_escalated,
          created_at,
          closed_at,
          priority,
           (case priority
                when 'Not Filled' then 0
                when 'Low/Medium' then 1
                when 'Medium/Low' then 2
                when 'Standard' then 3
                when 'P3 - Medium' then 4
                when 'High' then 5
                when 'Urgent' then 6
                else null
                end
           ) as priority_val,
          avg(priority_val) over (partition by account_id order by created_at) as avg_priority,
          severity_c,
          avg(severity_c) over (partition by account_id order by created_at) as avg_severity,
          account_id 
  from    "QLIK_ANALYTICS"."CUSTOMER_SUCCESS"."CASE" 
  where   is_closed = 'TRUE'
),

account_and_case_analyze_aggregate_to_comment as (
select cc.id, cc.parent_id, cc.created_at, 
    Row_number() over (partition by cc.id order by aac.created_at desc) as n_seq,
    aac.n_escalated,
    aac.n_case,
    aac.prop_escalated,
    aac.avg_priority,
    aac.avg_severity,
    aac.account_id,
    aac.created_at,
    aac.closed_at,
    aac.has_escalated_val
  
  from case_comments cc left join account_and_case_analyze aac
    on cc.parent_id = aac.id
    and cc.created_at >= aac.created_at //closed_at
  qualify n_seq = 1
),



associated_jira_aggregate_to_comment as (
select  cc.id, cc.parent_id, cc.created_at, aja.created_at as aja_created_at,
        Row_number() over (partition by cc.id order by aja.created_at desc) as n_seq,
        count(aja.id) over (partition by cc.id order by aja.created_at desc) as n_jira_tickets_up_to_comment,
        max(aja.bug_weight_c) over (partition by cc.id order by aja.created_at desc) as bug_weight_max_up_to_comment,
        avg(aja.bug_weight_c) over (partition by cc.id order by aja.created_at desc) as bug_weight_avg_up_to_comment
  from case_comments cc left join "QLIK_ANALYTICS"."CUSTOMER_SUCCESS"."CASE_DEFECT_C" aja
      on cc.parent_id = aja.case_c
      and aja.created_at <= cc.created_at
  qualify n_seq = 1
),

severity_priority as (
select  case_id, created_at, created_date,
          (case when field = 'Severity__c' then new_Value
           end) as severity,
          (case when field = 'Priority' then
            case new_value
                when 'Low/Medium' then 1
                when 'High' then 2
                when 'Urgent' then 3
                else null
                end
           end) as priority
  from    "QLIK_ANALYTICS"."CUSTOMER_SUCCESS"."CASE_HISTORY" 
  where severity is not null 
        or priority is not null
),

severity_priority_aggregate_to_comment as (
select  cc.id, cc.parent_id, cc.created_at, spa.created_at as spa_created_at,
        Row_number() over (partition by cc.id order by spa.created_at desc) as n_seq,
        max(severity) over (partition by cc.id order by spa.created_at desc) as severity_max_up_to_comment,
        avg(severity) over (partition by cc.id order by spa.created_at desc) as severity_avg_up_to_comment,
        count(severity) over (partition by cc.id order by spa.created_at desc) as severity_count_up_to_comment,
        severity as severity_latest_up_to_comment,
        max(priority) over (partition by cc.id order by spa.created_at desc) as priority_max_up_to_comment,
        avg(priority) over (partition by cc.id order by spa.created_at desc) as priority_avg_up_to_comment,
        count(priority) over (partition by cc.id order by spa.created_at desc) as priority_count_up_to_comment,
        priority as priority_latest_up_to_comment
  from case_comments cc left join severity_priority spa
      on cc.parent_id = spa.case_id 
      and spa.created_at <= cc.created_at
  qualify n_seq = 1
)

//severity_priority_aggregate_to_comment as (
//select  cc.id, cc.parent_id, cc.created_at, spa.created_at
////        max(severity_max) over (partition by cc.id order by cc.created_at) as severity_max_up_to_comment,
////        max(priority_max) over (partition by cc.id order by cc.created_at) as priority_max_up_to_comment
//  from case_comments cc left join severity_priority_aggregate spa 
//      on cc.parent_id = spa.case_id 
//      and spa.created_at = ( select spa2.created_at 
//                          from severity_priority_aggregate spa2
//                          where spa2.created_at <= cc.created_at
//                          order by spa2.created_at desc
//                          limit 1
//        )
//      
//)

,      
output as (    
select cc.id,
        cc.parent_id,
        cc.created_at,
        cc.created_date,
        cc.comment_body,
        cc.is_published,
        cc.customer_comment_boo,
        cc.n_comment,
        cc.n_customer_comment,
        cc.n_support_comment,
        aac.closed_at as case_analyze_close_at,
        aac.has_escalated_val,
        aac.n_escalated,
        aac.n_case,
        aac.prop_escalated,
        aac.avg_priority,
        aac.avg_severity,
        aac.account_id,
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
    left join account_and_case_analyze_aggregate_to_comment aac on cc.id = aac.id
    left join associated_jira_aggregate_to_comment aja on cc.id = aja.id
    left join severity_priority_aggregate_to_comment spa on cc.id = spa.id
where has_escalated_val is not null
),

output_customer_comment_only as (
    select * from output
  where customer_comment_boo = True
),

ouput_customer_last_comment as (
    select *, 
      Row_number() over (partition by parent_id order by created_at desc) as n_seq
  from output_customer_comment_only 
  qualify n_seq = 1
),

final_table as ( SELECT 1 + 1 AS result
)


//select count(severity)/ count(*), 
//        count(priority)/ count(*), 
//        count(distinct case_id)/ count(*)  
//        from severity_priority

//select * from output

//select count(*) from associated_jira_aggregate_to_comment

//select *
//from severity_priority
//where case_id = '5003z00002h0A8rAAE'
        
      
//select * from output
//where parent_id = '5003z00002aOpY1AAK'

//select count(distinct a.parent_id) from output a left join severity_priority b
//where a.parent_id = b.case_id

//select count (distinct case_id) from severity_priority

//select count(distinct account_id), count(distinct id), count(distinct parent_id) from output


select * from output


//select count(*), count(distinct id) from account_and_case_analyze_aggregate_to_comment




