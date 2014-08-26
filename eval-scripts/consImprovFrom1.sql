.mode csv
.header on
.width 20 20 20 20
.load sqlite_extensions

with main as (
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
        e.inputFile, c.rank, c.consistency as cons
    from
        experiments e
        join covers c on e.id=c.id
    where
        numCovers = 5
        and (thCov=1.0 or thCov is null)
        and (resultSelector is null or resultSelector = 'DiversifyingGreedyResultSelector')
        and method <> 'GeneticSetCovererZ'
),
better as (
    select
        t1.inputFile, t1.scenario, t1.identifier, t2.rank, t2.cons, t1.cons as cons_rank1
    from
        main t1 join main t2
            on t1.inputFile=t2.inputFile and t1.scenario=t2.scenario and t1.identifier=t2.identifier
            and t2.rank > 0 and t1.rank=0
    where
        t1.cons < t2.cons
),
maxima as (
    select inputFile, scenario, identifier, max(cons) as maximum
    from better
    group by inputFile, scenario, identifier
),
best as (
    select distinct b.inputFile, b.scenario, b.identifier, b.cons as cons_max, b.cons_rank1
    from better b join maxima m
        on b.inputFile=m.inputFile and b.scenario=m.scenario and b.identifier=m.identifier
        and b.cons = m.maximum
)
select
    case
        when identifier = 'GenQ-Divf-Def-0.0-1.0' then 'Genetic'
        when identifier = 'Gr*-Divf-Def-0.0-1.0' then 'Greedy*'
        when identifier = 'Gr-Def-Def-0.0-1.0' then 'Greedy'
        when identifier = 'TKGrouping-Def-Def--' then 'TopRanked'
    end as algorithm,
    round(count(*)/22.0,2) as "impr.Scen.", --22 is number of queries
    round(avg(cons_max-cons_rank1),2) as "avg.Improv.",
    round(stdev(cons_max-cons_rank1), 2) as "stdev.Improv."
from best
group by identifier;
