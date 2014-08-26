.header on
.mode csv

select
    method  || '-' || ifnull(resultSelector, '') || '-' || ifnull(picker, '') || '-' ||
    case
        when distanceMeasure = 'InverseSimByEntityDistance' then 'InvSim'
        when distanceMeasure = 'UsageJaccardDistance' then 'UsageJacc'
        else ''
    end
    || '-E-' || numEntities
    || '-s-' || ifnull(cast(scale as int), '')
        as identifier,
    numCovers,
    avg(runTime) as runTime
from experiments
where
    substr(inputFile,17) not in ('b','c','d')
    and (scale = 10 or scale is null)
    and numEntities = 20
    -- and numResults > 75.0
    and method <>'GroupingSetCoverer'
group by identifier, numCovers;