.header on
.mode csv

select
    method || '-' || ifnull(resultSelector, '') || '-' || ifnull(picker, '') || '-' ||
    case
        when distanceMeasure = 'InverseSimByEntityDistance' then 'InvSim'
        when distanceMeasure = 'UsageJaccardDistance' then 'UsageJacc'
        else ''
    end
    || '-nC-' || numCovers
    || '-s-' || ifnull(cast(scale as int), '')
        as identifier,
    numEntities,
    avg(runTime) as runTime,
    avg(numResults) as numResults
from experiments
where
    substr(inputFile,17) not in ('b','c','d')
    and (scale = 10 or scale is null)
    and (numCovers = 10 or method = 'GroupingSetCoverer')
    -- and numResults > 75.0
    and numEntities > 5
group by identifier, numEntities;
