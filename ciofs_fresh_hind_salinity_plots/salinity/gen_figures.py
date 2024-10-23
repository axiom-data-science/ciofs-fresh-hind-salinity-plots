import cartopy
import cmocean
import datetime
import matplotlib.pyplot as plt
import numpy as np
import os
from dask.distributed import Client
from pathlib import Path
import xarray as xr

# Initialize Dask client
def init_dask_client():
    return Client(n_workers=os.cpu_count()-2, threads_per_worker=1)

# Get the yearday from a date string
def get_yearday(date_input):
    date_object = datetime.datetime.strptime(date_input, "%Y-%m-%d")
    return date_object.timetuple().tm_yday

# Convert date format
def convert_date_format(date_input):
    date_object = datetime.datetime.strptime(date_input, "%Y-%m")
    return date_object.strftime("%B %Y")

# Load datasets
def load_datasets(year, month, input_dir_hindcast, input_dir_freshwater):
    input_files_hindcast = []
    input_files_freshwater = []
    num_days = (datetime.datetime(year, month % 12 + 1, 1) - datetime.timedelta(days=1)).day if month < 12 else 31

    for day in range(1, num_days + 1):
        date_str = f"{year}-{month:02d}-{day:02d}"
        yearday = get_yearday(date_str)
        fname_hindcast = f"{input_dir_hindcast}/{year}/axiom.ciofs.fields.hindcast.{year}_{yearday:04d}.nc"
        fname_freshwater = f"{input_dir_freshwater}/{year}/axiom.ciofs.fields.hindcast.{year}_{yearday:04d}.nc"
        input_files_hindcast.append(fname_hindcast)
        input_files_freshwater.append(fname_freshwater)

    print(input_files_hindcast)
    print(input_files_freshwater)

    ds_hc = xr.open_mfdataset(input_files_hindcast, **xr_kwargs)
    ds_fw = xr.open_mfdataset(input_files_freshwater, **xr_kwargs)
    return ds_hc, ds_fw

# Plotting function
def plot_salinity(ds_hc, ds_fw, year, month):
    var_hc = ds_hc['salt'].isel(s_rho=-1).mean(axis=0)
    var_fw = ds_fw['salt'].isel(s_rho=-1).mean(axis=0)
    std_hc = ds_hc['salt'].isel(s_rho=-1).std(axis=0)
    std_fw = ds_fw['salt'].isel(s_rho=-1).std(axis=0)

    figsize = (12,12)
    fs = 12
    proj = cartopy.crs.LambertAzimuthalEqualArea(central_longitude=-151, central_latitude=59) # these values impact the results a lot so they need to be accurate for this projection
    pc = cartopy.crs.PlateCarree()

    fig, axes = plt.subplots(2, 3, figsize=figsize, layout="constrained",subplot_kw=dict(projection=proj, frameon=True),)
    # title
    fig.suptitle(f"{convert_date_format(f'{year}-{month:02d}')} Monthly Surface Salinity\n", fontsize=16)

    # TOP ROW
    label = "Mean Surface Salinity"
    # Calculate the plotting parameters across both NWGOA and CIOFS with robust=True to get the best values, and save the vmin/vmax
    cmap_params = xr.plot.utils._determine_cmap_params(np.vstack((var_hc.values, var_fw.values)), robust=True)

    # Top-left
    mappable = var_hc.plot(x="lon_rho", y="lat_rho", vmin=cmap_params["vmin"], vmax=cmap_params["vmax"], ax=axes[0,0], transform=pc, 
                                            cmap=cmocean.cm.haline, add_colorbar=False)
    axes[0,0].add_feature(cartopy.feature.LAND.with_scale('10m'), facecolor='0.8')
    gl = axes[0,0].gridlines(draw_labels=True, x_inline=False, y_inline=False, xlocs=np.arange(-156,-148,2))
        # manipulate `gridliner` object to change locations of labels
    gl.top_labels = False
    gl.bottom_labels = False
    gl.right_labels = False
    axes[0,0].set_title('River-forced CIOFS hindcast')

    # Top-middle
    mappable = var_fw.plot(x="lon_rho", y="lat_rho", vmin=cmap_params["vmin"], vmax=cmap_params["vmax"], ax=axes[0,1], transform=pc, 
                                            cmap=cmocean.cm.haline, add_colorbar=False)
    axes[0,1].add_feature(cartopy.feature.LAND.with_scale('10m'), facecolor='0.8')
    gl = axes[0,1].gridlines(draw_labels=True, x_inline=False, y_inline=False, xlocs=np.arange(-156,-148,2))
        # manipulate `gridliner` object to change locations of labels
    gl.top_labels = False
    gl.bottom_labels = False
    gl.left_labels = False
    gl.right_labels = False
    axes[0,1].set_title('Watershed-forced CIOFS hindcast')

    # try to use the mappable saved from the plot above to form your colormap — if this doesn't work I have a workaround for it
    # the "ax" parameter in the colorbar call actually sets which subplots it takes space from for the colorbar and it works great
    cbar = fig.colorbar(mappable, ax=axes[0,:2], orientation="horizontal", shrink=0.5)
    cbar.set_label(label, fontsize=fs)
    cbar.ax.tick_params(axis="both", labelsize=fs)

    # Top-right
    mappable = (var_hc-var_fw).plot(x="lon_rho", y="lat_rho", ax=axes[0,2], transform=pc, 
                                            cmap=cmocean.cm.balance, add_colorbar=False)
    axes[0,2].add_feature(cartopy.feature.LAND.with_scale('10m'), facecolor='0.8')
    gl = axes[0,2].gridlines(draw_labels=True, x_inline=False, y_inline=False, xlocs=np.arange(-156,-148,2))
        # manipulate `gridliner` object to change locations of labels
    gl.top_labels = False
    gl.bottom_labels = False
    gl.left_labels = False
    gl.right_labels = False
    axes[0,2].set_title('River-forced minus Watershed-forced')

    cbar = fig.colorbar(mappable, ax=axes[0,2], orientation="horizontal", shrink=1.0)
    cbar.set_label(label, fontsize=fs)
    cbar.ax.tick_params(axis="both", labelsize=fs)

    # BOTTOM ROW
    label = "Surface Salinity\n Standard Deviation"
    # Calculate the plotting parameters across both NWGOA and CIOFS with robust=True to get the best values, and save the vmin/vmax
    cmap_params = xr.plot.utils._determine_cmap_params(np.vstack((std_hc.values, std_fw.values)), robust=True)

    # Bottom-left
    mappable = std_hc.plot(x="lon_rho", y="lat_rho", vmin=cmap_params["vmin"], vmax=cmap_params["vmax"], ax=axes[1,0], transform=pc, 
                                            cmap=cmocean.cm.haline, add_colorbar=False)
    axes[1,0].add_feature(cartopy.feature.LAND.with_scale('10m'), facecolor='0.8')
    gl = axes[1,0].gridlines(draw_labels=True, x_inline=False, y_inline=False, xlocs=np.arange(-156,-148,2))
        # manipulate `gridliner` object to change locations of labels
    gl.top_labels = False
    gl.right_labels = False
    axes[1,0].set_title('')

    # Bottom-middle
    mappable = std_fw.plot(x="lon_rho", y="lat_rho", vmin=cmap_params["vmin"], vmax=cmap_params["vmax"], ax=axes[1,1], transform=pc, 
                                            cmap=cmocean.cm.haline, add_colorbar=False)
    axes[1,1].add_feature(cartopy.feature.LAND.with_scale('10m'), facecolor='0.8')
    gl = axes[1,1].gridlines(draw_labels=True, x_inline=False, y_inline=False, xlocs=np.arange(-156,-148,2))
        # manipulate `gridliner` object to change locations of labels
    gl.top_labels = False
    gl.left_labels = False
    gl.right_labels = False
    axes[1,1].set_title('')

    # try to use the mappable saved from the plot above to form your colormap — if this doesn't work I have a workaround for it
    # the "ax" parameter in the colorbar call actually sets which subplots it takes space from for the colorbar and it works great
    cbar = fig.colorbar(mappable, ax=axes[1,:2], orientation="horizontal", shrink=0.5)
    cbar.set_label(label, fontsize=fs)
    cbar.ax.tick_params(axis="both", labelsize=fs)

    # Bottom-right
    mappable = (std_hc-std_fw).plot(x="lon_rho", y="lat_rho", ax=axes[1,2], transform=pc, 
                                            cmap=cmocean.cm.balance, add_colorbar=False)
    axes[1,2].add_feature(cartopy.feature.LAND.with_scale('10m'), facecolor='0.8')
    gl = axes[1,2].gridlines(draw_labels=True, x_inline=False, y_inline=False, xlocs=np.arange(-156,-148,2))
        # manipulate `gridliner` object to change locations of labels
    gl.top_labels = False
    gl.left_labels = False
    gl.right_labels = False
    axes[1,2].set_title('')

    cbar = fig.colorbar(mappable, ax=axes[1,2], orientation="horizontal", shrink=1.0)
    cbar.set_label(label, fontsize=fs)
    cbar.ax.tick_params(axis="both", labelsize=fs)

    plt.savefig(f"salinity_{year}-{month:02d}.png", dpi=300, bbox_inches="tight")

# Main execution
if __name__ == "__main__":
    c = init_dask_client()
    input_dir_hindcast = "/mnt/vault/ciofs/HINDCAST"
    input_dir_freshwater = "/mnt/vault/ciofs/HINDCAST_FRESHWATER"

    xr_kwargs = dict(
        parallel=True,
        engine='netcdf4',
        data_vars='minimal',
        coords='minimal',
        compat='override',
        combine='nested',
        concat_dim=['ocean_time'],
        decode_cf=True,
        decode_times=True,
    )

    for year in [2003, 2004, 2005, 2006, 2012, 2013, 2014]:
    #for year in [2006, 2012, 2013, 2014]:
    #for year in [2013]:
        for month in range(1, 13):
            if Path(f"salinity_{year}-{month:02d}.png").exists():
                continue
            print(f"Creating plots for {year}-{month:02d}...")
            ds_hc, ds_fw = load_datasets(year, month, input_dir_hindcast, input_dir_freshwater)
            plot_salinity(ds_hc, ds_fw, year, month)
