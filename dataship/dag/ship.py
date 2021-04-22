from dataship.dag.utils import *

def merge_l8(data_folder,tile_id):
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
                    merge_rasters(band_list,tile_id,out_fn)

