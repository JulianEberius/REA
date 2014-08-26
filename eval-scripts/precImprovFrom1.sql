.mode csv
.header on
.width 20 20 20 20
.load sqlite_extensions

with byEntity as (
    select
        e.concept || " " ||
        case
            when attribute = 'revenues|revenue|sales' then 'revenues'
            when attribute = 'profit|earnings|profits' then 'profit'
            when attribute = 'established|founded' then 'founded'
            when attribute = 'population|inhabitants' then 'population'
            when attribute = 'size|area' then 'area'
            else attribute
        end
        || " " ||
        case
            when inputFile like '%andom%' then 'random'
            else 'top'
        end as scenario,

        case
            when method = 'GreedySetCoverer' then 'Gr'
            when method = 'MultiGreedySetCoverer' then 'Gr*'
            when method = 'GroupingSetCoverer' then 'Grouping'
            when method = 'TopKGroupingSetCoverer' then 'TKGrouping'
            when method = 'GeneticSetCovererQ' then 'GenQ'
            when method = 'GeneticSetCovererZ' then 'GenZ'
        end
        || '-' ||
        case
            when resultSelector = 'ReplacingResultSelectorWithReplacement(ReplacementDecider13)' then 'RR13'
            when resultSelector = 'ReplacingResultSelector(Repla6cementDecider13)' then 'R13'
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
        end || '-' || coalesce(thCons, '') || '-' || coalesce(thCov, '')
        as identifier,
        e.inputFile, cp.entity, cp.rank, a.correct as correct
    from
            experiments e
            left join coverParts cp on e.id=cp.id
            left join answers a on cp.identifier = a.identifier and cp.entity=a.entity and e.attribute=a.forAttribute
    where
        numCovers = 5
        and (thCov=1.0 or thCov is null)
        and (resultSelector is null or resultSelector = 'DiversifyingGreedyResultSelector')
        and method <> 'GeneticSetCovererZ'
),
avgPrecs as (
    select
        t1.inputFile, t1.scenario, t1.identifier, t1.rank, avg(t1.correct) as corr
    from
        byEntity t1
    group by
        inputFile, scenario, identifier, rank
    having corr > 0.5
),
better as (
    select
        t1.inputFile, t1.scenario, t1.identifier, t2.rank, t2.corr, t1.corr as corr_rank1
    from
        avgPrecs t1 join avgPrecs t2
            on t1.inputFile=t2.inputFile and t1.scenario=t2.scenario and t1.identifier=t2.identifier
            and t2.rank > 0 and t1.rank=0
    where
        t1.corr < t2.corr
),
maxima as (
    select inputFile, scenario, identifier, max(corr) as maximum
    from better
    group by inputFile, scenario, identifier
),
best as (
    select distinct b.inputFile, b.scenario, b.identifier, b.corr as corr_max, b.corr_rank1, b.corr - b.corr_rank1, (b.corr / b.corr_rank1)-1.0
    from better b join maxima m
        on b.inputFile=m.inputFile and b.scenario=m.scenario and b.identifier=m.identifier
        and b.corr = m.maximum
    order by b.identifier
)
select
    case
        when identifier = 'GenQ-Divf-Def-0.0-1.0' then 'Genetic'
        when identifier = 'Gr*-Divf-Def-0.0-1.0' then 'Greedy*'
        when identifier = 'Gr-Def-Def-0.0-1.0' then 'Greedy'
        when identifier = 'TKGrouping-Def-Def--' then 'TopRanked'
    end as algorithm,
    round(count(*)/22.0,2) as "impr.Scen.", --22 is number of queries
    round(avg((corr_max/corr_rank1)-1.0),2) as "avg.Improv.",
    round(stdev((corr_max/corr_rank1)-1.0), 2) as "stdev.Improv."
from best
group by identifier;
