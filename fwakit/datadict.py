# Nested dictionaries defining:
# - input .gdb.zip files
# - table name
# - table alias
# - table id
# - fields to be indexed in table
# - whether table is 'grouped' (there are tables for each watershed group in the gdb)

filedef = {
   'FWA_BC.gdb.zip': {
      'fwa_assessment_watersheds_poly': {
         'alias': 'assessment_watersheds',
         'id': 'watershed_feature_id',
         'index_fields': [
            'watershed_group_code',
            'gnis_name_1',
            'waterbody_id',
            'waterbody_key',
            'watershed_key'
         ]
      },
      'fwa_bays_and_channels_poly': {
         'alias': 'bays_and_channels',
         'id': 'bay_and_channel_id',
         'index_fields': [
            'gnis_name'
         ]
      },
      'fwa_edge_type_codes': {
         'alias': 'edge_type_codes',
         'id': 'edge_type',
         'index_fields': {

         }
      },
      'fwa_glaciers_poly': {
         'alias': 'glaciers',
         'id': 'waterbody_poly_id',
         'index_fields': [
            'blue_line_key',
            'watershed_key',
            'waterbody_key',
            'watershed_group_code',
            'gnis_name_1'
         ]
      },
      'fwa_lakes_poly': {
         'alias': 'lakes',
         'id': 'waterbody_poly_id',
         'index_fields': [
            'blue_line_key',
            'watershed_key',
            'waterbody_key',
            'watershed_group_code',
            'gnis_name_1'
         ]
      },
      'fwa_manmade_waterbodies_poly': {
         'alias': 'manmade_waterbodies',
         'id': 'waterbody_poly_id',
         'index_fields': [
            'blue_line_key',
            'watershed_key',
            'waterbody_key',
            'watershed_group_code',
            'gnis_name_1'
         ]
      },
      'fwa_obstructions_sp': {
         'alias': 'obstructions',
         'id': 'obstruction_id',
         'index_fields': [
            'linear_feature_id',
            'blue_line_key',
            'watershed_key',
            'obstruction_type',
            'watershed_group_code',
            'gnis_name'
         ]
      },
      'fwa_rivers_poly': {
         'alias': 'rivers',
         'id': 'waterbody_poly_id',
         'index_fields': [
            'blue_line_key',
            'watershed_key',
            'waterbody_key',
            'watershed_group_code',
            'gnis_name_1'
         ]
      },
      'fwa_streams_20k_50k': {
         'alias': 'streams_20k_50k',
         'id': 'stream_20k_50k_id',
         'index_fields': [
            'watershed_group_id_20k',
            'linear_feature_id_20k',
            'watershed_code_50k'
         ]
      },
      'fwa_waterbodies_20k_50k': {
         'alias': 'waterbodies_20k_50k',
         'id': 'waterbody_20k_50k_id',
         'index_fields': [
            'waterbody_type_20k',
            'watershed_group_id_20k',
            'waterbody_poly_id_20k',
            'fwa_watershed_code_20k',
            'watershed_code_50k'
         ]
      },
      'fwa_waterbody_type_codes': {
         'alias': 'waterbody_type_codes',
         'id': 'waterbody_type',
         'index_fields': {

         }
      },
      'fwa_watershed_groups_poly': {
         'alias': 'groups',
         'id': 'watershed_group_id',
         'index_fields': [
            'watershed_group_code'
         ]
      },
      'fwa_wetlands_poly': {
         'alias': 'wetlands',
         'id': 'waterbody_poly_id',
         'index_fields': [
            'blue_line_key',
            'watershed_key',
            'waterbody_key',
            'watershed_group_code',
            'gnis_name_1'
         ]
      }
   },

   'FWA_LINEAR_BOUNDARIES_SP.gdb.zip': {
      'fwa_linear_boundaries_sp': {
         'alias': 'linear_boundaries',
         'grouped': True,
         'id': 'linear_feature_id',
         'index_fields': [
            'edge_type',
            'blue_line_key',
            'watershed_key',
            'waterbody_key',
            'watershed_group_code'
         ]
      }
   },

   'FWA_STREAM_NETWORKS_SP.gdb.zip': {
      'fwa_stream_networks_sp': {
         'alias': 'streams',
         'grouped': True,
         'id': 'linear_feature_id',
         'index_fields': [
            'edge_type',
            'blue_line_key',
            'watershed_key',
            'waterbody_key',
            'watershed_group_code',
            'gnis_name',
            'stream_order'
         ]
      }
   },

   'FWA_WATERSHEDS_POLY.gdb.zip': {
      'fwa_watersheds_poly_sp': {
         'alias': 'watersheds',
         'grouped': True,
         'id': 'watershed_feature_id',
         'index_fields': [
            'gnis_name_1',
            'waterbody_id',
            'waterbody_key',
            'watershed_key',
            'watershed_group_code'
         ]
      }
   }
}