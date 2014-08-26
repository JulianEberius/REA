.header on
.mode csv

select
    method || '-' || ifnull(resultSelector, '') || '-' || ifnull(picker, '') || '-' ||
    case
        when distanceMeasure = 'InverseSimByEntityDistance' then 'InvSim'
        when distanceMeasure = 'UsageJaccardDistance' then 'UsageJacc'
        else ''
    end
    || '-E-' || numEntities
    || '-nC-' || numCovers
    -- || '-s-' || scale
        as identifier,
    ifnull(cast(scale as int), 1) as scale,
    avg(runTime) as runTime
from experiments
where substr(inputFile,17) not in ('b','c','d')
    and (numCovers = 10 or method = 'GroupingSetCoverer')
    and numEntities = 20
    -- and numResults > 75.0
    and (scale > 0 or scale is null)
    -- and scale is not null
group by identifier, scale;

