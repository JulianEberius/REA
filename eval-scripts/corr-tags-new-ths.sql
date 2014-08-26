.mode csv
.header on

with allTags as (
    select e.concept, e.inputFile, e.attribute, e.method || '-' || ifnull(e.resultSelector, '') || '-' || ifnull(e.picker, '') as method,
    ifnull(e.thCons, '') as thCons, ifnull(e.thCov, '') as thCov, cp.rank, t.tag
    from
        experiments e
        join coverParts cp on e.id=cp.id
        join answers a on cp.identifier = a.identifier and cp.entity=a.entity and e.attribute=a.forAttribute
        join tags t on a.identifier = t.identifier and a.forAttribute = t.forAttribute
    where
        e.numCovers = 5 and
        t.tag not like 'domain:%' and
        -- attribute not like 'established%' and
        ((thCons=0.0 and thCov=1.0) or (thCov is null and thCons is null))
    ),
tagCounts as (
    select w1.concept, w1.inputFile, w1.attribute, w1.method, w1.thCons, w1.thCov, w1.rank, count(distinct w1.tag) as tag_count
    from allTags w1
    group by w1.concept, w1.inputFile, w1.attribute, w1.method, w1.thCons, w1.thCov, w1.rank
),
cumulativeTagCounts as (
    select m.concept, m.inputFile, m.attribute, m.method, m.thCons, m.thCov, m.rank, m.tag_count as tag_count, count(distinct w.tag) as cumulative_tag_count
    from tagCounts m join allTags w on w.concept=m.concept and w.inputFile=m.inputFile and w.attribute=m.attribute and w.method=m.method and w.thCons=m.thCons and w.thCov=m.thCov and w.rank<=m.rank
    group by m.concept, m.inputFile, m.attribute, m.method, m.thCons, m.thCov, m.rank, m.tag_count
    order by m.concept, m.inputFile, m.attribute, m.method, m.thCons, m.thCov, m.rank
),
final as (
    select method||'-'||thCons||'-'||thCov as identifier, rank, avg(tag_count) as atRank, avg(cumulative_tag_count) as uptoRank from cumulativeTagCounts group by identifier, rank
    ),
final_by_domain as (
    select method||'-'||concept||'-'||attribute||'-'||thCons||'-'||thCov as identifier, rank, avg(tag_count) as atRank, avg(cumulative_tag_count) as uptoRank from cumulativeTagCounts group by identifier, rank
    )

select * from final;
