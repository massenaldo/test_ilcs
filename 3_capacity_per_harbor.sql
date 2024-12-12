-- Create table 
create table public.port_container_capacity (
	port_id int4 null, -- ID pelabuhan.
	year int4 null, -- Tahun data.
	month int4 null, -- Bulan data.
	capacity int4 -- Kapasitas kontainer (TEU): 
);

create table public.port_details (
	port_id int4 null, -- ID pelabuhan.
	port_name VARCHAR null, -- Nama pelabuhan.
	location VARCHAR null -- Lokasi pelabuhan.
);

--Kuartal 1 (Q1): Januari, Februari, Maret
--Kuartal 2 (Q2): April, Mei, Juni
--Kuartal 3 (Q3): Juli, Agustus, September
--Kuartal 4 (Q4): Oktober, November, Desember

-- Total capacity per location per quarter
select pd."location",
	case 
		when pcc.month in (1,2,3) then 'Q1'
		when pcc.month in (4,5,6) then 'Q2'
		when pcc.month in (7,8,9) then 'Q3'
		when pcc.month in (10,11,12) then 'Q4'
	end as quartal,
	sum(pcc.capacity) as total_capacity
from public.port_container_capacity pcc
left join public.port_details pd on pcc.port_id = pd.port_id 
group by pd.location,case 
		when pcc.month in (1,2,3) then 'Q1'
		when pcc.month in (4,5,6) then 'Q2'
		when pcc.month in (7,8,9) then 'Q3'
		when pcc.month in (10,11,12) then 'Q4'
	end
order by quartal;

-- Average capacity yearly and percentage different capacity with each others
with calculate_average_ as (
	select pd."location", pcc.year, avg(pcc.capacity) as avg_capacity
	from public.port_container_capacity pcc
	left join public.port_details pd on pcc.port_id = pd.port_id
	group by pd."location", pcc.year
), calculate_total_avg_ as (
	select ca.year, sum(avg_capacity) as total_avg
	from calculate_average_ ca
	group by ca.year
)select ca.location, ca.year, round(ca.avg_capacity,2) as avg_capacity, round(100*(ca.avg_capacity/nullif(coalesce(cta.total_avg,0),0)),2) as delta
from calculate_average_ ca
left join calculate_total_avg_ cta on ca.year = cta.year;
;

-- Harbor with increased total capacity per month
with set_lagging_ as (
	select
		pd."location",
		pd.port_name,
		pcc."month",
		pcc.capacity,
		lag(pcc.capacity,1) over (partition by pd."location",pd.port_name order by pcc."month") as lagging,
		case 
			when lag(pcc.capacity,1) over (partition by pd."location",pd.port_name order by pcc."month") < capacity then 'improve'
			when lag(pcc.capacity,1) over (partition by pd."location",pd.port_name order by pcc."month") = capacity then 'stabil'
			else 'decrease'
		end as flag
	from
		public.port_container_capacity pcc
	left join public.port_details pd on
		pcc.port_id = pd.port_id
)
select 
	sl.port_name,
	sl."location",
	sl."month",
	sl.capacity,
	'+'::varchar || (sl.capacity - sl.lagging)::varchar as increase
from set_lagging_ sl
where flag = 'improve';