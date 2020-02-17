insert Record
{RID=1, KeyVal=91678, 
Columns=[SID: 91678, G1: 80, G2: 60]}
--------
update for Record with KeyVal=91678
TID = 2 ** 64 - 1
{query_cols = [SID: 91678, G1: 100, G2: None]}
--------
update for Record with KeyVal=91678
TID = 2 ** 64 - 2
{query_cols = [SID: 91678, G1: None, G2: 99]}
----------
select for "" = 
query_cols = 1 1 1 // read everything 
{RID=1, KeyVal=91678, G1: 100, G2: 99} 
// schema for base record: 0 1 1 


