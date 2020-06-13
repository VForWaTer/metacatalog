# Configuration file for the Sphinx documentation builder.
#
# This file only contains a selection of the most common options. For a full
# list see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#
import os
import sys
sys.path.insert(0, os.path.abspath(os.path.join('.', '..')))

def get_version():
    B = os.path.abspath(os.path.dirname(__file__))
    with open(os.path.join(B, '..', '..', 'VERSION'), 'r') as f:
        return f.read().strip()

# -- Project information -----------------------------------------------------

project = 'Metacatalog'
copyright = '2020, Mirko Mälicke'
author = 'Mirko Mälicke <mirko.maelicke@kit.edu>'

# The full version, including alpha/beta/rc tags
release = get_version()


# -- General configuration ---------------------------------------------------

# Add any Sphinx extension module names here, as strings. They can be
# extensions coming with Sphinx (named 'sphinx.ext.*') or your custom
# ones.
extensions = [
    'sphinx.ext.napoleon',
    'sphinx.ext.autodoc',
    'sphinx.ext.mathjax',
    'nbsphinx',
    'sphinx_gallery.load_style'
#    'sphinx.ext.inheritance_diagram',
]

# Add any paths that contain templates here, relative to this directory.
templates_path = ['_templates']

# List of patterns, relative to source directory, that match files and
# directories to ignore when looking for source files.
# This pattern also affects html_static_path and html_extra_path.
exclude_patterns = []


# -- Options for HTML output -------------------------------------------------

# The theme to use for HTML and HTML Help pages.  See the documentation for
# a list of builtin themes.
#
#html_theme = 'alabaster'
html_theme = 'pydata_sphinx_theme'
html_logo = '_static/brand.png'

# These paths are either relative to html_static_path
# or fully qualified paths (eg. https://...)
html_css_files = [
    'css/custom.css',
]

html_theme_options = {
#    'github_user': 'VForWaTer',
#    'github_repo': 'metacatalog',
#    'github_button': True,
#    'fixed_sidebar': True,
#    'body_text_align': 'justify'
    'github_url': 'https://github.com/VForWaTer/metacatalog'
}

html_context = {
    'github_user': 'VForWaTer',
    'github_repo': 'metacatalog',
    'github_version': 'master',
    'doc_path': 'docs/source'
}

html_short_title = 'MetaCatalog'

"""html_sidebars = {
    '**': [
        'about.html',
        'navigation.html',
        'relations.html',
        'searchbox.html',
        'donate.html'
    ]
}"""

# Add any paths that contain custom static files (such as style sheets) here,
# relative to this directory. They are copied after the builtin static files,
# so a file named "default.css" will overwrite the builtin "default.css".
html_static_path = ['_static']
