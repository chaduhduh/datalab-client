""""
Provides various conversion functions that are useful in a cross match context
"""

#TODO: this could probably just go into utils or something?
def arcs_to_deg(arcs=None):
    """ convert arcseconds to degrees """
    return arcs/3600
