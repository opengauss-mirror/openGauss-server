# Case: Creating an Appropriate Index<a name="EN-US_TOPIC_0000001382270689"></a>

## Symptom<a name="en-us_topic_0073253825_en-us_topic_0040046522_section52209455"></a>

Query the information about all personnel in the sales department.

```
SELECT staff_id,first_name,last_name,employment_id,state_name,city 
FROM staffs,sections,states,places 
WHERE sections.section_name='Sales' 
AND staffs.section_id = sections.section_id 
AND sections.place_id = places.place_id 
AND places.state_id = states.state_id 
ORDER BY staff_id;
```

## Optimization Analysis<a name="en-us_topic_0073253825_en-us_topic_0040046522_section123050"></a>

The original execution plan is as follows before creating the **places.place\_id** and **states.state\_id** indexes:

```
                                            QUERY PLAN
---------------------------------------------------------------------------------------------------
 Sort  (cost=129.74..131.18 rows=576 width=136)
   Sort Key: staffs.staff_id
   ->  Hash Join  (cost=70.54..103.33 rows=576 width=136)
         Hash Cond: (states.state_id = places.state_id)
         ->  Seq Scan on states  (cost=0.00..22.38 rows=1238 width=36)
         ->  Hash  (cost=69.38..69.38 rows=93 width=108)
               ->  Hash Join  (cost=42.41..69.38 rows=93 width=108)
                     Hash Cond: (places.place_id = sections.place_id)
                     ->  Seq Scan on places  (cost=0.00..21.67 rows=1167 width=40)
                     ->  Hash  (cost=42.21..42.21 rows=16 width=76)
                           ->  Hash Join  (cost=24.66..42.21 rows=16 width=76)
                                 Hash Cond: (staffs.section_id = sections.section_id)
                                 ->  Seq Scan on staffs  (cost=0.00..15.37 rows=537 width=76)
                                 ->  Hash  (cost=24.59..24.59 rows=6 width=8)
                                       ->  Seq Scan on sections  (cost=0.00..24.59 rows=6 width=8)
                                             Filter: (section_name = 'Sales'::text)
(16 rows)
```

The optimized execution plan is as follows (two indexes have been created on the **places.place\_id** and **states.state\_id** columns):

```
                                                QUERY PLAN
-----------------------------------------------------------------------------------------------------------
 Sort  (cost=119.76..121.20 rows=576 width=136)
   Sort Key: staffs.staff_id
   ->  Hash Join  (cost=70.14..93.35 rows=576 width=136)
         Hash Cond: (staffs.section_id = sections.section_id)
         ->  Seq Scan on staffs  (cost=0.00..15.37 rows=537 width=76)
         ->  Hash  (cost=67.43..67.43 rows=217 width=68)
               ->  Nested Loop  (cost=24.66..67.43 rows=217 width=68)
                     ->  Hash Join  (cost=24.66..51.06 rows=35 width=40)
                           Hash Cond: (places.place_id = sections.place_id)
                           ->  Seq Scan on places  (cost=0.00..21.67 rows=1167 width=40)
                           ->  Hash  (cost=24.59..24.59 rows=6 width=8)
                                 ->  Seq Scan on sections  (cost=0.00..24.59 rows=6 width=8)
                                       Filter: (section_name = 'Sales'::text)
                     ->  Index Scan using states_state_id_idx on states  (cost=0.00..0.41 rows=6 width=36)
                           Index Cond: (state_id = places.state_id)
(15 rows)
```
