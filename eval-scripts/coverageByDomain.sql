.mode csv
.header on
.width 10 20 20

select
    e.concept || " " ||
    case
        when attribute = 'revenues|revenue|sales' then 'revenues'
        when attribute = 'profit|earnings|profits' then 'profit'
        when attribute = 'established|founded' then 'founded'
        when attribute = 'population|inhabitants' then 'population'
        when attribute = 'population growth' then 'pop. growth'
        when attribute = 'size|area' then 'area'
        else attribute
    end
    ||
    case
        when inputFile like '%compa%andom%' then ' rand'
        when inputFile like '%compa%' then ' top'
        when inputFile like '%cit%andom%' then ' rand'
        when inputFile like '%cit%' then ' top'
        else ''
    end  as scenario,
    avg(numCoverableEntities / cast(numEntities as real)) as covered,
    numResults
from
        experiments e
where
    numCovers = 5
group by
    scenario
;