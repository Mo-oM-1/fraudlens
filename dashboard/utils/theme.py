"""
Theme configuration for FraudLens Dashboard
Supports Dark and Light mode toggle
"""
import streamlit as st


# Theme definitions
THEMES = {
    "dark": {
        "name": "Dark",
        "icon": "🌙",
        "backgroundColor": "#0e1117",
        "secondaryBackgroundColor": "#1a1a2e",
        "textColor": "#fafafa",
        "primaryColor": "#74b9ff",
        "cardBackground": "#1a1a2e",
        "cardBorder": "#333",
        "subtitleColor": "#888",
        "footerColor": "#666",
        "gradientStart": "#74b9ff",
        "gradientEnd": "#a29bfe",
    },
    "light": {
        "name": "Light",
        "icon": "☀️",
        "backgroundColor": "#ffffff",
        "secondaryBackgroundColor": "#f0f2f6",
        "textColor": "#1a1a2e",
        "primaryColor": "#0066cc",
        "cardBackground": "#f8f9fa",
        "cardBorder": "#dee2e6",
        "subtitleColor": "#6c757d",
        "footerColor": "#868e96",
        "gradientStart": "#0066cc",
        "gradientEnd": "#6f42c1",
    }
}


def init_theme():
    """Initialize theme in session state if not present."""
    if "theme" not in st.session_state:
        st.session_state.theme = "dark"


def get_current_theme():
    """Get the current theme configuration."""
    init_theme()
    return THEMES[st.session_state.theme]


def toggle_theme():
    """Toggle between dark and light theme."""
    init_theme()
    st.session_state.theme = "light" if st.session_state.theme == "dark" else "dark"


def render_theme_toggle():
    """Render the theme toggle button at the bottom of the sidebar."""
    init_theme()
    theme = get_current_theme()

    next_theme = "light" if st.session_state.theme == "dark" else "dark"
    next_icon = THEMES[next_theme]["icon"]

    # Place toggle button at the bottom of the sidebar
    with st.sidebar:
        # Add spacer to push button to bottom
        st.markdown("<div style='flex-grow: 1;'></div>", unsafe_allow_html=True)
        st.divider()

        col1, col2, col3 = st.columns([1, 1, 1])
        with col2:
            if st.button(
                next_icon,
                key="theme_toggle",
                help=f"Switch to {THEMES[next_theme]['name']} mode"
            ):
                toggle_theme()
                st.rerun()


def apply_theme_css():
    """Apply CSS based on current theme."""
    theme = get_current_theme()

    css = f"""
    <style>
        /* Main background override */
        .stApp {{
            background-color: {theme['backgroundColor']};
        }}

        /* Sidebar background */
        [data-testid="stSidebar"] {{
            background-color: {theme['secondaryBackgroundColor']};
        }}

        /* Text colors */
        .stApp, .stMarkdown, p, span, label {{
            color: {theme['textColor']} !important;
        }}

        /* Header gradient */
        .main-header {{
            font-size: 3rem;
            font-weight: bold;
            text-align: center;
            padding: 2rem 0;
            background: linear-gradient(90deg, {theme['gradientStart']}, {theme['gradientEnd']});
            -webkit-background-clip: text;
            -webkit-text-fill-color: transparent;
        }}

        /* Subtitle */
        .subtitle {{
            text-align: center;
            color: {theme['subtitleColor']} !important;
            font-size: 1.2rem;
            margin-bottom: 2rem;
        }}

        /* Feature cards */
        .feature-card {{
            background: {theme['cardBackground']};
            border-radius: 10px;
            padding: 1.5rem;
            margin: 0.5rem 0;
            border: 1px solid {theme['cardBorder']};
        }}

        /* Stat highlight */
        .stat-highlight {{
            font-size: 2rem;
            font-weight: bold;
            color: {theme['primaryColor']};
        }}

        /* Metric cards */
        [data-testid="stMetric"] {{
            background-color: {theme['secondaryBackgroundColor']};
            border-radius: 8px;
            padding: 1rem;
        }}

        /* Footer */
        .footer-text {{
            text-align: center;
            color: {theme['footerColor']} !important;
            font-size: 0.9rem;
        }}

        /* Plotly charts - transparent background */
        .js-plotly-plot .plotly {{
            background-color: transparent !important;
        }}

        /* DataFrames */
        [data-testid="stDataFrame"] {{
            background-color: {theme['secondaryBackgroundColor']};
        }}

        /* Selectbox and inputs */
        .stSelectbox, .stTextInput, .stNumberInput {{
            background-color: {theme['secondaryBackgroundColor']};
        }}

        /* Expander */
        .streamlit-expanderHeader {{
            background-color: {theme['secondaryBackgroundColor']};
        }}

        /* Tabs */
        .stTabs [data-baseweb="tab-list"] {{
            background-color: {theme['secondaryBackgroundColor']};
        }}

        /* Sidebar collapse/expand button */
        [data-testid="collapsedControl"] {{
            color: {theme['textColor']} !important;
            background-color: {theme['secondaryBackgroundColor']} !important;
        }}

        [data-testid="collapsedControl"] svg {{
            fill: {theme['textColor']} !important;
            stroke: {theme['textColor']} !important;
        }}

        /* Sidebar toggle button (arrow) */
        button[kind="header"] {{
            color: {theme['textColor']} !important;
        }}

        button[kind="header"] svg {{
            fill: {theme['textColor']} !important;
            stroke: {theme['textColor']} !important;
        }}

        /* All SVG icons in sidebar area */
        [data-testid="stSidebar"] svg,
        [data-testid="stSidebarCollapsedControl"] svg {{
            fill: {theme['textColor']} !important;
            stroke: {theme['textColor']} !important;
        }}

        /* Sidebar navigation arrows and controls */
        [data-testid="stSidebarNavItems"] svg {{
            fill: {theme['textColor']} !important;
        }}

        /* Header buttons */
        header button svg {{
            fill: {theme['textColor']} !important;
            stroke: {theme['textColor']} !important;
        }}

        /* Main menu button */
        [data-testid="stMainMenu"] svg {{
            fill: {theme['textColor']} !important;
        }}

        /* Page link icons */
        [data-testid="stSidebarNavLink"] svg {{
            fill: {theme['textColor']} !important;
        }}

        /* Sidebar collapse button - always visible */
        [data-testid="stSidebarCollapseButton"] {{
            opacity: 1 !important;
            visibility: visible !important;
            color: {theme['textColor']} !important;
        }}

        [data-testid="stSidebarCollapseButton"] button {{
            opacity: 1 !important;
            visibility: visible !important;
        }}

        [data-testid="stSidebarCollapseButton"] svg {{
            fill: {theme['textColor']} !important;
            stroke: {theme['textColor']} !important;
            opacity: 1 !important;
        }}

        /* Collapsed sidebar expand button - always visible */
        [data-testid="collapsedControl"] {{
            opacity: 1 !important;
            visibility: visible !important;
        }}

        /* Force sidebar controls to always show */
        section[data-testid="stSidebar"] > div > div > div > div > div > button {{
            opacity: 1 !important;
            visibility: visible !important;
        }}

        /* Arrow button in sidebar header */
        [data-testid="stSidebar"] header {{
            opacity: 1 !important;
        }}

        [data-testid="stSidebar"] header button {{
            opacity: 1 !important;
            visibility: visible !important;
        }}

        [data-testid="stSidebar"] header button svg {{
            fill: {theme['textColor']} !important;
            stroke: {theme['textColor']} !important;
        }}
    </style>
    """

    st.markdown(css, unsafe_allow_html=True)


def get_plotly_theme():
    """Get Plotly theme configuration based on current theme."""
    theme = get_current_theme()

    return {
        "paper_bgcolor": "rgba(0,0,0,0)",
        "plot_bgcolor": "rgba(0,0,0,0)",
        "font": {
            "color": theme["textColor"]
        },
        "xaxis": {
            "gridcolor": theme["cardBorder"],
            "linecolor": theme["cardBorder"],
        },
        "yaxis": {
            "gridcolor": theme["cardBorder"],
            "linecolor": theme["cardBorder"],
        }
    }
