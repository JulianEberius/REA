.mode column
.header on
.width 35 65 35
select
    case
        when method = 'GreedySetCoverer' then 'Gr'
        when method = 'MultiGreedySetCoverer' then 'Gr*'
        when method = 'GroupingSetCoverer' then 'Grouping'
        when method = 'TopKGroupingSetCoverer' then 'TKGrouping'
        when method = 'GeneticSetCovererQ' then 'GenQ'
        when method = 'GeneticSetCovererZ' then 'GenZ'
    end as method,
    resultSelector,
    -- case
    --     when resultSelector = 'ReplacingResultSelectorWithReplacement(ReplacementDecider13)' then 'RR13'
    --     when resultSelector = 'ReplacingResultSelector(Repla6cementDecider13)' then 'R13'
    --     when resultSelector = 'ScoreOnlyGreedyResultSelector' then 'SO'
    --     when resultSelector = 'DiversityOnlyGreedyResultSelector' then 'DO'
    --     when resultSelector = 'DiversifyingGreedyResultSelector' then 'Divf'
    --     else 'Def'
    -- end as resultSelector,
    -- case
    --     when picker = 'NoReusePicker' then 'NoReuse'
    --     when picker = 'CoherenceOnlyExponentialDiversityPicker' then 'CO'
    --     when picker = 'ScoreOnlyExponentialDiversityPicker' then 'SO'
    --     when picker = 'DiversityOnlyExponentialDiversityPicker' then 'DO'
    --     when picker = 'ExponentialDiversityDynamicPicker' then 'Dyn'
    --     else 'Def'
    -- end as picker,
    thCons||'-'||thCov as ths,
    -- rank,
    round(avg(e.consistency),2) as cons,
    -- round(avg(e.diversityUpTo),2) as div9,
    round(avg(e.diversity),2) as div,
    round(avg(e.inherentScore),2) as inhS,
    round(avg(e.minSources),2) as minS,
    round(avg(e.coverage),2) as cov
    -- avg((select count(*) from covers c where c.id=e.id)) as cc
    -- round((avg(consistency) + avg(inherentScore))/2.0, 2) as score,
    -- round((avg(coverage) + avg(minSources))/2.0, 2) as covQuality,
    -- round(avg(runTime),2) as runTime
from experiments e
     -- join covers c on e.id=c.id
where
    -- (thCov = 1.0 or thCov is null) and (thCons = 0.0 or thCons is null)
    numEntities=20
    and (scale = 10 or scale is null)
    -- and (thCov >= .5 or thCov is null)
    -- and (thCons = 0.4 or thCons is null)
     -- (thCov = 1.0 or thCov is null)
    -- and numCovers = 10
    and (numCovers = 10 or method like 'GroupingSetCoverer')
    -- and rank=9
    -- and concept = 'company'
    and (resultSelector = 'DiversifyingGreedyResultSelector' or resultSelector is null)
    -- and attribute like 'reven%'
    -- and picker != 'UsagePicker'
    -- and (numCovers = 3 or numCovers = 5)
    -- and (resultSelector = "ReplacingResultSelectorWithReplacement(ReplacementDecider11)" or resultSelector is null)
    -- and (resultSelector in ("GreedyResultSelector", "ReplacingResultSelectorWithReplacement(ReplacementDecider11)") or resultSelector is null)

group by method, resultSelector, ths
order by method, resultSelector, ths;
-- order by div*score*covQuality desc;
-- order by cons*div desc;
-- order by div*score desc;
-- order by score*covQuality desc;
