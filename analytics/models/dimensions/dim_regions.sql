with contract_regions(region_code) as (
    values ('us-east'), ('us-west'), ('eu-west'), ('ap-south')
)

select
    md5(region_code) as region_key,
    region_code
from contract_regions
