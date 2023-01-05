--CREATE OR REPLACE VIEW locations_realdata AS
select 
	st_asewkt(t.point_location),
	t.*,
	st_area(t.geom::geography) as "area_sqm"
from 
(SELECT 
entries.id,
	case when entries.location is not null then
		entries.location
	else 
		case when spatial_scales.extent is not null then
			st_centroid(spatial_scales.extent)
		else 
 			case when entries.geom is not null then
 				st_centroid(entries.geom)
 			else
				null::geometry
 			end
		end
	end  
	as point_location
	,
 	case when entries.geom is not null then
 		entries.geom
 	else
		case when spatial_scales.extent is not null then
			spatial_scales.extent
		else 
			entries.location
		end 
	end
 	as "geom"
FROM entries
LEFT JOIN datasources ON entries.datasource_id = datasources.id
LEFT JOIN spatial_scales ON spatial_scales.id = datasources.spatial_scale_id
 ) as t

order by t.id asc