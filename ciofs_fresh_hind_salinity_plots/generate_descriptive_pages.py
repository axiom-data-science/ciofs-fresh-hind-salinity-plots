"""Generate mean plot page."""

import subprocess
import intake
import nbformat as nbf
import report_utils as ru
import numpy as np


def generate_means_pages(varname, nicename, not_in_jupyter_book):
    """Generate pages of descriptive monthly means plots.
    
    This uses plots produced by scripts from Melanie at /mnt/vault/ciofs/daily_ciofs_nwgoa/output_files
    """
    
    nb = nbf.v4.new_notebook()
    # nb['cells'] = [ru.page_utils.imports_cell(),]

    text = f"""\
(page:means-{varname})=
# Monthly {nicename} Means


[100MB zipfile of plots](https://files.axds.co/ciofs_fresh/zip/means_{varname}.zip)
"""

    nb['cells'].append(ru.page_utils.text_cell(text))


    years = [2003,2004,2005,2006,2012,2013,2014]
    for year in years:
        # import pdb; pdb.set_trace()
        nb['cells'].append(ru.page_utils.text_cell(ru.page_utils.header_text(year, header=2)))

        fignames = sorted(ru.utils.PAGE_DIR(varname).glob(f"{varname}_{year}*.png"))
        if not not_in_jupyter_book:
            text = f"""
`````{{dropdown}} Monthly means

"""
        else:
            text = f"""
Monthly means

"""
            
            
        for i, figname in enumerate(fignames):
            text += f"""
{ru.utils.mk_fig_wide(figname, "", "", not_in_jupyter_book)}
"""
#             text += f"""

# {ru.utils.mk_fig_wide(figname.relative_to(ru.utils.PAGE_DIR(varname)), "", "")}
# """

        if not not_in_jupyter_book:
            text += f"""
`````
"""
                
        nb["cells"].append(ru.page_utils.text_cell(text))

    file = ru.utils.PAGE_DIR(varname).with_suffix('.ipynb')
    nbf.write(nb, file)

    # Run jupytext command
    subprocess.run(["jupytext", "--to", "myst", file])


# Generate pages
if __name__ == "__main__":

    not_in_jupyter_book=False
    varnames = ["salinity"]
    nicenames = ["Surface Salinity"]
    for varname, nicename in zip(varnames, nicenames):
        generate_means_pages(varname, nicename, not_in_jupyter_book=not_in_jupyter_book)