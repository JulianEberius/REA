.mode csv
.header on
.width 10 20 20

create temp table tmp (scenario, identifier, corr real);

.import data/byDomainCorrRelaxedTopOrTail.csv tmp
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
            when method = 'GeneticSetCovererXE' then 'GenXE'
            when method = 'GeneticSetCovererY' then 'GenY'
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
        cp.entity, cp.rank, a.correct as correct
        -- (select avg(ac.correct) from coverParts cpc left join answers ac on e.attribute=ac.forAttribute and cpc.identifier = ac.identifier and cpc.entity=ac.entity
        --         where e.id=cpc.id
        --         and cpc.entity = cp.entity
        --         and cpc.rank <= cp.rank
        --     ) as avg_correct

    from
            experiments e
            left join coverParts cp on e.id=cp.id
            left join answers a on cp.identifier = a.identifier and cp.entity=a.entity and e.attribute=a.forAttribute
    where
        numCovers = 5
        and (thCov=1.0 or thCov is null)
        and (resultSelector is null or resultSelector = 'DiversifyingGreedyResultSelector')
        and method <> 'GeneticSetCovererZ'),
tmp as (
    select
        -- scenario, identifier || "-" || "rank_5" as identifier, avg(cumulative_correct / cast(cumulative_correct_cnt as real)) as corr
        scenario, identifier || "-" || "rank_5" as identifier, avg(correct) as corr
    from
        byEntity
    group by
        scenario, identifier

    union all

    select
        scenario, identifier || "-" ||  "rank_1" as identifier, avg(correct) as corr
    from
        byEntity
    where
        rank = 0
    group by
        scenario, identifier
)

select identifier, avg(corr), stdev(corr) from tmp where (identifier like '%Divf%0.0-1.0-rank_1' or identifier like '%---rank_1' or identifier like 'Gr-Def%0.0-1.0-rank_1') and cast(corr as real) > 0.5 group by identifier
union all
select identifier, avg(corr), stdev(corr) from tmp where (identifier like '%Divf%0.0-1.0-rank_5' or identifier like '%---rank_5' or identifier like 'Gr-Def%0.0-1.0-rank_5') and cast(corr as real) > 0.5  group by identifier;