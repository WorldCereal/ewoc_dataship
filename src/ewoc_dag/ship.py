import os

from ewoc_dag.utils import merge_rasters, get_bounds, list_path_bands

def merge_l8(data_folder,tile_id):
    bounds = get_bounds(tile_id)
    merge_dict = list_path_bands(data_folder)
    for date in merge_dict:
        for path in merge_dict[date]:
            for band in merge_dict[date][path]:
                print(date,'--',path,'--',band)
                band_list = merge_dict[date][path][band]
                if len(band_list) < 2:
                    pass
                else:
                    out_fn = os.path.join(data_folder,date+'_'+path+'_'+band+'.tif')
                    merge_rasters(band_list,bounds,out_fn)