#!/bin/bash

db_file=$1
scale_factor=$2

sqlite3 $db_file <<EOF
.header on
.mode csv

select
    case
        when method = 'GreedySetCoverer' then 'Gr'
        when method = 'MultiGreedySetCoverer' then 'Gr*'
        when method = 'GroupingSetCoverer' then 'Grouping'
        when method = 'TopKGroupingSetCoverer' then 'TKGrouping'
        when method = 'GeneticSetCovererXE' then 'GenXE'
        when method = 'GeneticSetCovererY' then 'GenY'
        when method = 'GeneticSetCovererZ' then 'GenZ'
        when method = 'GeneticSetCovererQ' then 'GenQ'
        when method = 'GeneticSetCovererStupid' then 'GenSt'
    end
    || '-' ||
    case
        when resultSelector = 'ReplacingResultSelectorWithReplacement(ReplacementDecider13)' then 'RR13'
        when resultSelector = 'ReplacingResultSelector(ReplacementDecider13)' then 'R13'
        when resultSelector = 'ScoreOnlyGreedyResultSelector' then 'SO'
        when resultSelector = 'DiversityOnlyGreedyResultSelector' then 'DO'
        when resultSelector = 'DiversifyingGreedyResultSelector' then 'Divf'
        else 'Def'
    end
    || '-' ||
    case
        when picker = 'NoReusePicker' then 'NoReuse'
        when picker = 'CoherenceOnlyExponentialDiversityPicker' then 'CO'
        when picker = 'ScoreOnlyExponentialDiversityPicker' then 'SO'
        when picker = 'DiversityOnlyExponentialDiversityPicker' then 'DO'
        when picker = 'ExponentialDiversityDynamicPicker' then 'Dyn'
        else 'Def'
    end
    || '-' ||
    case
        when distanceMeasure = 'InverseSimByEntityDistance' then 'InvSim'
        when distanceMeasure = 'UsageJaccardDistance' then 'UsageJacc'
        else ''
    end
    || '-' || coalesce(thCons, '') || '-' || coalesce(thCov, '') as identifier,
    rank,
    round(avg(c.consistency),2) as cons,
    round(avg(c.inherentScore),2) as inhS,
    round(avg(c.minSources),2) as minS,
    round(avg(c.coverage),2) as cov,
    round((avg(c.consistency) + avg(c.inherentScore))/2.0, 2) as score,
    round((avg(c.coverage) + avg(c.minSources))/2.0, 2) as covQuality,
    round(avg(c.diversityUpTo),2) as div,
    round(avg(e.runTime),2) as runTime
from
    experiments e join covers c on c.id=e.id
where
    --numResults > 75.0 and
    (numCovers = 10 or method = 'GroupingSetCoverer') and
    (scale = $scale_factor or scale is null) and
    -- and ((thCov=1.0) or (thCov is null))
    numEntities = 20
    -- ((thCov=1.0 and thCons=0.2) or (thCov=0.0 and thCons=0.6) or (thCov is null and thCons is null)) and
    -- ((thCov=1.0 and thCons=0.0) or (thCov=0.0 and thCons=0.6) or (thCov is null and thCons is null))
group by
    identifier, rank
order by
    identifier desc, rank asc;
EOF

