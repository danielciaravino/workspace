

select final.customer_id
      ,final.customer_name
      ,final.cust_first_inv_date
      ,round(final.ar_pct_curr,3) as ar_pct_curr
      ,final.delivery_count
      ,final.net_sales_amt
      ,round(final.cust_mar_qual,3) as cust_mar_qual
from (
select	coalesce(pa11.customer_id, pa12.customer_id, pa13.customer_id)  customer_id,
	coalesce(pa11.customer_1_name, pa12.customer_1_name, pa13.customer_1_name)  customer_name,
	coalesce(pa11.first_invoice_date, pa12.first_invoice_date, pa13.first_invoice_date)  cust_first_inv_date,
  safe_divide(pa13.WJXBFS2,pa13.WJXBFS3) as ar_pct_curr,
  pa12.WJXBFS1 as delivery_count,
  pa11.WJXBFS1 as net_sales_amt,
  safe_divide(pa11.WJXBFS2,pa11.WJXBFS1) as cust_mar_qual,
	pa13.WJXBFS2  WJXBFS3,
	pa11.WJXBFS2  WJXBFS4,
	pa13.WJXBFS3  WJXBFS6,
	pa12.WJXBFS2  WJXBFS7
from	(select	a14.customer_id  customer_id,
		max(a14.customer_1_name)  customer_1_name,
		a13.first_invoice_date  first_invoice_date,
		sum(a11.net_sales_amt)  WJXBFS1,
		sum(a11.customer_margin_amt)  WJXBFS2
	from	gcp-gfs-datalake-edw-prd.edw_views.f_sales_customer	a11
		join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
		  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
		join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
		  on 	(a12.customer_sk = a13.customer_sk)
		join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
		  on 	(a12.customer_sk = a14.customer_sk)
		join	(select	coalesce(pa11.first_invoice_date, pa12.first_invoice_date)  first_invoice_date,
			coalesce(pa11.customer_id, pa12.customer_id)  customer_id,
			coalesce(pa11.customer_1_name, pa12.customer_1_name)  customer_1_name
		from	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				sum((Case when a11.past_due_ind = 0 then a11.balance_amt else 0 end))  WJXBFS1,
				sum(a11.balance_amt)  WJXBFS2,
				count(distinct a11.snapshot_date)  WJXBFS3
			from	gcp-gfs-datalake-edw-prd.edw_views.f_ar_balance_snap	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a15
				  on 	(a11.snapshot_date = a15.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a16
				  on 	(a12.sales_hierarchy_sk = a16.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a16.business_unit_id in ('237')
			 and a15.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa11
			full outer join	(select	pa11.customer_id  customer_id,
				pa11.customer_1_name  customer_1_name,
				pa11.first_invoice_date  first_invoice_date,
				pa11.WJXBFS3  WJXBFS1
			from	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				sum((Case when a11.past_due_ind = 0 then a11.balance_amt else 0 end))  WJXBFS1,
				sum(a11.balance_amt)  WJXBFS2,
				count(distinct a11.snapshot_date)  WJXBFS3
			from	gcp-gfs-datalake-edw-prd.edw_views.f_ar_balance_snap	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a15
				  on 	(a11.snapshot_date = a15.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a16
				  on 	(a12.sales_hierarchy_sk = a16.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a16.business_unit_id in ('237')
			 and a15.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa11
			)	pa12
			  on 	(pa11.customer_id = pa12.customer_id and 
			pa11.first_invoice_date = pa12.first_invoice_date)
			left outer join	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				sum(a11.net_sales_amt)  WJXBFS1
			from	gcp-gfs-datalake-edw-prd.edw_views.f_sales_customer	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a15
				  on 	(a11.posted_date = a15.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a16
				  on 	(a12.sales_hierarchy_sk = a16.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a16.business_unit_id in ('237')
			 and a15.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa13
			  on 	(coalesce(pa11.customer_id, pa12.customer_id) = pa13.customer_id and 
			coalesce(pa11.first_invoice_date, pa12.first_invoice_date) = pa13.first_invoice_date)
		where	((pa13.WJXBFS1 > 0
		 and CASE when ((pa11.WJXBFS1 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) / CASE when (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) = 0 then NULL else (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) end) IS NULL then 0 else ((pa11.WJXBFS1 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) / CASE when (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) = 0 then NULL else (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) end) end is not null))
		)	pa15
		  on 	(a13.first_invoice_date = pa15.first_invoice_date and 
		a14.customer_id = pa15.customer_id)
		join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a16
		  on 	(a11.posted_date = a16.date)
		join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a17
		  on 	(a12.sales_hierarchy_sk = a17.sales_hierarchy_sk)
	where	(a14.market_segment_code in ('01')
	 and a14.customer_type_code in ('0001')
	 and a17.business_unit_id in ('237')
	 and a16.fiscal_sales_day_before_last_completed_day <=  89)
	group by	1,
		3
	)	pa11
	full outer join	(select	pa11.customer_id  customer_id,
		pa11.customer_1_name  customer_1_name,
		pa11.first_invoice_date  first_invoice_date,
		pa11.WJXBFS1  WJXBFS1,
		pa11.WJXBFS2  WJXBFS2
	from	(select	coalesce(pa11.customer_id, pa12.customer_id)  customer_id,
			coalesce(pa11.customer_1_name, pa12.customer_1_name)  customer_1_name,
			coalesce(pa11.first_invoice_date, pa12.first_invoice_date)  first_invoice_date,
			pa11.WJXBFS1  WJXBFS1,
			pa12.WJXBFS1  WJXBFS2
		from	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				count(distinct (case when (a11.route_nbr='0' or a11.delivery_seq='0') then null else concat(a11.delivery_date,a11.ship_plant_id,a11.route_nbr,a11.delivery_seq,a11.ship_to_customer_sk) end))  WJXBFS1
			from	gcp-gfs-datalake-edw-prd.edw_views.f_delivery_detail	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	(select	coalesce(pa11.first_invoice_date, pa12.first_invoice_date)  first_invoice_date,
			coalesce(pa11.customer_id, pa12.customer_id)  customer_id,
			coalesce(pa11.customer_1_name, pa12.customer_1_name)  customer_1_name
		from	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				sum((Case when a11.past_due_ind = 0 then a11.balance_amt else 0 end))  WJXBFS1,
				sum(a11.balance_amt)  WJXBFS2,
				count(distinct a11.snapshot_date)  WJXBFS3
			from	gcp-gfs-datalake-edw-prd.edw_views.f_ar_balance_snap	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a15
				  on 	(a11.snapshot_date = a15.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a16
				  on 	(a12.sales_hierarchy_sk = a16.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a16.business_unit_id in ('237')
			 and a15.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa11
			full outer join	(select	pa11.customer_id  customer_id,
				pa11.customer_1_name  customer_1_name,
				pa11.first_invoice_date  first_invoice_date,
				pa11.WJXBFS3  WJXBFS1
			from	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				sum((Case when a11.past_due_ind = 0 then a11.balance_amt else 0 end))  WJXBFS1,
				sum(a11.balance_amt)  WJXBFS2,
				count(distinct a11.snapshot_date)  WJXBFS3
			from	gcp-gfs-datalake-edw-prd.edw_views.f_ar_balance_snap	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a15
				  on 	(a11.snapshot_date = a15.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a16
				  on 	(a12.sales_hierarchy_sk = a16.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a16.business_unit_id in ('237')
			 and a15.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa11
			)	pa12
			  on 	(pa11.customer_id = pa12.customer_id and 
			pa11.first_invoice_date = pa12.first_invoice_date)
			left outer join	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				sum(a11.net_sales_amt)  WJXBFS1
			from	gcp-gfs-datalake-edw-prd.edw_views.f_sales_customer	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a15
				  on 	(a11.posted_date = a15.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a16
				  on 	(a12.sales_hierarchy_sk = a16.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a16.business_unit_id in ('237')
			 and a15.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa13
			  on 	(coalesce(pa11.customer_id, pa12.customer_id) = pa13.customer_id and 
			coalesce(pa11.first_invoice_date, pa12.first_invoice_date) = pa13.first_invoice_date)
		where	((pa13.WJXBFS1 > 0
		 and CASE when ((pa11.WJXBFS1 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) / CASE when (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) = 0 then NULL else (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) end) IS NULL then 0 else ((pa11.WJXBFS1 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) / CASE when (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) = 0 then NULL else (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) end) end is not null))
		)	pa15
				  on 	(a13.first_invoice_date = pa15.first_invoice_date and 
				a14.customer_id = pa15.customer_id)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a16
				  on 	(a11.delivery_date = a16.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a17
				  on 	(a12.sales_hierarchy_sk = a17.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a17.business_unit_id in ('237')
			 and a16.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa11
			full outer join	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				count(distinct a11.snapshot_date)  WJXBFS1,
				sum((Case when a11.past_due_ind = 0 then a11.balance_amt else 0 end))  WJXBFS2,
				sum(a11.balance_amt)  WJXBFS3
			from	gcp-gfs-datalake-edw-prd.edw_views.f_ar_balance_snap	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	(select	coalesce(pa11.first_invoice_date, pa12.first_invoice_date)  first_invoice_date,
			coalesce(pa11.customer_id, pa12.customer_id)  customer_id,
			coalesce(pa11.customer_1_name, pa12.customer_1_name)  customer_1_name
		from	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				sum((Case when a11.past_due_ind = 0 then a11.balance_amt else 0 end))  WJXBFS1,
				sum(a11.balance_amt)  WJXBFS2,
				count(distinct a11.snapshot_date)  WJXBFS3
			from	gcp-gfs-datalake-edw-prd.edw_views.f_ar_balance_snap	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a15
				  on 	(a11.snapshot_date = a15.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a16
				  on 	(a12.sales_hierarchy_sk = a16.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a16.business_unit_id in ('237')
			 and a15.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa11
			full outer join	(select	pa11.customer_id  customer_id,
				pa11.customer_1_name  customer_1_name,
				pa11.first_invoice_date  first_invoice_date,
				pa11.WJXBFS3  WJXBFS1
			from	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				sum((Case when a11.past_due_ind = 0 then a11.balance_amt else 0 end))  WJXBFS1,
				sum(a11.balance_amt)  WJXBFS2,
				count(distinct a11.snapshot_date)  WJXBFS3
			from	gcp-gfs-datalake-edw-prd.edw_views.f_ar_balance_snap	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a15
				  on 	(a11.snapshot_date = a15.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a16
				  on 	(a12.sales_hierarchy_sk = a16.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a16.business_unit_id in ('237')
			 and a15.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa11
			)	pa12
			  on 	(pa11.customer_id = pa12.customer_id and 
			pa11.first_invoice_date = pa12.first_invoice_date)
			left outer join	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				sum(a11.net_sales_amt)  WJXBFS1
			from	gcp-gfs-datalake-edw-prd.edw_views.f_sales_customer	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a15
				  on 	(a11.posted_date = a15.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a16
				  on 	(a12.sales_hierarchy_sk = a16.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a16.business_unit_id in ('237')
			 and a15.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa13
			  on 	(coalesce(pa11.customer_id, pa12.customer_id) = pa13.customer_id and 
			coalesce(pa11.first_invoice_date, pa12.first_invoice_date) = pa13.first_invoice_date)
		where	((pa13.WJXBFS1 > 0
		 and CASE when ((pa11.WJXBFS1 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) / CASE when (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) = 0 then NULL else (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) end) IS NULL then 0 else ((pa11.WJXBFS1 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) / CASE when (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) = 0 then NULL else (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) end) end is not null))
		)	pa15
				  on 	(a13.first_invoice_date = pa15.first_invoice_date and 
				a14.customer_id = pa15.customer_id)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a16
				  on 	(a11.snapshot_date = a16.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a17
				  on 	(a12.sales_hierarchy_sk = a17.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a17.business_unit_id in ('237')
			 and a16.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa12
			  on 	(pa11.customer_id = pa12.customer_id and 
			pa11.first_invoice_date = pa12.first_invoice_date)
		)	pa11
		join	(select	coalesce(pa11.first_invoice_date, pa12.first_invoice_date)  first_invoice_date,
			coalesce(pa11.customer_id, pa12.customer_id)  customer_id,
			coalesce(pa11.customer_1_name, pa12.customer_1_name)  customer_1_name
		from	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				sum((Case when a11.past_due_ind = 0 then a11.balance_amt else 0 end))  WJXBFS1,
				sum(a11.balance_amt)  WJXBFS2,
				count(distinct a11.snapshot_date)  WJXBFS3
			from	gcp-gfs-datalake-edw-prd.edw_views.f_ar_balance_snap	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a15
				  on 	(a11.snapshot_date = a15.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a16
				  on 	(a12.sales_hierarchy_sk = a16.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a16.business_unit_id in ('237')
			 and a15.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa11
			full outer join	(select	pa11.customer_id  customer_id,
				pa11.customer_1_name  customer_1_name,
				pa11.first_invoice_date  first_invoice_date,
				pa11.WJXBFS3  WJXBFS1
			from	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				sum((Case when a11.past_due_ind = 0 then a11.balance_amt else 0 end))  WJXBFS1,
				sum(a11.balance_amt)  WJXBFS2,
				count(distinct a11.snapshot_date)  WJXBFS3
			from	gcp-gfs-datalake-edw-prd.edw_views.f_ar_balance_snap	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a15
				  on 	(a11.snapshot_date = a15.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a16
				  on 	(a12.sales_hierarchy_sk = a16.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a16.business_unit_id in ('237')
			 and a15.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa11
			)	pa12
			  on 	(pa11.customer_id = pa12.customer_id and 
			pa11.first_invoice_date = pa12.first_invoice_date)
			left outer join	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				sum(a11.net_sales_amt)  WJXBFS1
			from	gcp-gfs-datalake-edw-prd.edw_views.f_sales_customer	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a15
				  on 	(a11.posted_date = a15.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a16
				  on 	(a12.sales_hierarchy_sk = a16.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a16.business_unit_id in ('237')
			 and a15.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa13
			  on 	(coalesce(pa11.customer_id, pa12.customer_id) = pa13.customer_id and 
			coalesce(pa11.first_invoice_date, pa12.first_invoice_date) = pa13.first_invoice_date)
		where	((pa13.WJXBFS1 > 0
		 and CASE when ((pa11.WJXBFS1 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) / CASE when (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) = 0 then NULL else (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) end) IS NULL then 0 else ((pa11.WJXBFS1 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) / CASE when (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) = 0 then NULL else (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) end) end is not null))
		)	pa12
		  on 	(pa11.customer_id = pa12.customer_id and 
		pa11.first_invoice_date = pa12.first_invoice_date)
	)	pa12
	  on 	(pa11.customer_id = pa12.customer_id and 
	pa11.first_invoice_date = pa12.first_invoice_date)
	full outer join	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				count(distinct a11.snapshot_date)  WJXBFS1,
				sum((Case when a11.past_due_ind = 0 then a11.balance_amt else 0 end))  WJXBFS2,
				sum(a11.balance_amt)  WJXBFS3
			from	gcp-gfs-datalake-edw-prd.edw_views.f_ar_balance_snap	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	(select	coalesce(pa11.first_invoice_date, pa12.first_invoice_date)  first_invoice_date,
			coalesce(pa11.customer_id, pa12.customer_id)  customer_id,
			coalesce(pa11.customer_1_name, pa12.customer_1_name)  customer_1_name
		from	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				sum((Case when a11.past_due_ind = 0 then a11.balance_amt else 0 end))  WJXBFS1,
				sum(a11.balance_amt)  WJXBFS2,
				count(distinct a11.snapshot_date)  WJXBFS3
			from	gcp-gfs-datalake-edw-prd.edw_views.f_ar_balance_snap	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a15
				  on 	(a11.snapshot_date = a15.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a16
				  on 	(a12.sales_hierarchy_sk = a16.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a16.business_unit_id in ('237')
			 and a15.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa11
			full outer join	(select	pa11.customer_id  customer_id,
				pa11.customer_1_name  customer_1_name,
				pa11.first_invoice_date  first_invoice_date,
				pa11.WJXBFS3  WJXBFS1
			from	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				sum((Case when a11.past_due_ind = 0 then a11.balance_amt else 0 end))  WJXBFS1,
				sum(a11.balance_amt)  WJXBFS2,
				count(distinct a11.snapshot_date)  WJXBFS3
			from	gcp-gfs-datalake-edw-prd.edw_views.f_ar_balance_snap	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a15
				  on 	(a11.snapshot_date = a15.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a16
				  on 	(a12.sales_hierarchy_sk = a16.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a16.business_unit_id in ('237')
			 and a15.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa11
			)	pa12
			  on 	(pa11.customer_id = pa12.customer_id and 
			pa11.first_invoice_date = pa12.first_invoice_date)
			left outer join	(select	a14.customer_id  customer_id,
				max(a14.customer_1_name)  customer_1_name,
				a13.first_invoice_date  first_invoice_date,
				sum(a11.net_sales_amt)  WJXBFS1
			from	gcp-gfs-datalake-edw-prd.edw_views.f_sales_customer	a11
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_sales_area	a12
				  on 	(a11.customer_sales_area_sk = a12.customer_sales_area_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer	a13
				  on 	(a12.customer_sk = a13.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_customer_unit	a14
				  on 	(a12.customer_sk = a14.customer_sk)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a15
				  on 	(a11.posted_date = a15.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a16
				  on 	(a12.sales_hierarchy_sk = a16.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a16.business_unit_id in ('237')
			 and a15.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa13
			  on 	(coalesce(pa11.customer_id, pa12.customer_id) = pa13.customer_id and 
			coalesce(pa11.first_invoice_date, pa12.first_invoice_date) = pa13.first_invoice_date)
		where	((pa13.WJXBFS1 > 0
		 and CASE when ((pa11.WJXBFS1 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) / CASE when (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) = 0 then NULL else (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) end) IS NULL then 0 else ((pa11.WJXBFS1 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) / CASE when (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) = 0 then NULL else (pa11.WJXBFS2 / CASE when pa12.WJXBFS1 = 0 then NULL else pa12.WJXBFS1 end) end) end is not null))
		)	pa15
				  on 	(a13.first_invoice_date = pa15.first_invoice_date and 
				a14.customer_id = pa15.customer_id)
				join	gcp-gfs-datalake-edw-prd.edw_views.s_load_status_fiscal_sales_day	a16
				  on 	(a11.snapshot_date = a16.date)
				join	gcp-gfs-datalake-edw-prd.edw_views.d_sales_hierarchy	a17
				  on 	(a12.sales_hierarchy_sk = a17.sales_hierarchy_sk)
			where	(a14.market_segment_code in ('01')
			 and a14.customer_type_code in ('0001')
			 and a17.business_unit_id in ('237')
			 and a16.fiscal_sales_day_before_last_completed_day <=  89)
			group by	1,
				3
			)	pa13
	  on 	(coalesce(pa11.customer_id, pa12.customer_id) = pa13.customer_id and 
	coalesce(pa11.first_invoice_date, pa12.first_invoice_date) = pa13.first_invoice_date)
) final
join gcp-gfs-datalake-core-prd.sap__saphanadb__views_current.but000 but
  on (cast(but.partner as int64) = cast(final.customer_id as int64))
  where but.bu_group in ('0001')
  and but.xblck not in ('X')
order by 1
  # 40105

