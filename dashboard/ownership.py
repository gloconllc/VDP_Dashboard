# =============================================================================
#  Dana Point PULSE  |  Proprietary and Confidential
#  Copyright (c) 2026 GloCon Solutions LLC. All rights reserved.
#  Created and owned by John Picou, GloCon Solutions LLC, built for Visit Dana Point.
#  This code, its design, and the PULSE report are protected. Copying, sharing,
#  reuse, or redistribution of any part requires written permission.
#  Contact John Picou, GloCon Solutions LLC: john.picou@gloconsolutions.com
#  See NOTICE.md in the repository root.
# =============================================================================
"""
ownership.py, the one place the PULSE ownership notice is written.

Every generated page (the live app, the Insights report, the extra pieces, the
weekly PDF) carries this notice in its source: meta tags, an HTML comment,
and a hidden element, so anyone who views source or copies the page sees who
owns it and whom to contact. Change the wording here only.
"""
from __future__ import annotations

OWNER = "GloCon Solutions LLC"
AUTHOR = "John Picou"
CONTACT = "john.picou@gloconsolutions.com"
YEAR = 2026

NOTICE = (f"Dana Point PULSE. Copyright (c) {YEAR} {OWNER}. All rights reserved. "
          f"Created and owned by {AUTHOR}, {OWNER}, for Visit Dana Point. "
          f"Proprietary and confidential: copying, sharing, reuse, or redistribution of this report, "
          f"its code, or its design requires written permission. Contact {AUTHOR}, {OWNER}: {CONTACT}.")

HTML_COMMENT = f"<!-- {NOTICE} -->"

META_TAGS = (f'<meta name="author" content="{AUTHOR}, {OWNER}">'
             f'<meta name="copyright" content="Copyright (c) {YEAR} {OWNER}. All rights reserved.">'
             f'<meta name="owner" content="{OWNER}; contact {CONTACT}">')

# Hidden in the live app's page (not shown, present in the DOM and on copy).
HIDDEN_DIV = (f'<div id="pulse-ownership" hidden aria-hidden="true" data-owner="{OWNER}" '
              f'data-author="{AUTHOR}" data-contact="{CONTACT}">{NOTICE}</div>')


def head_block() -> str:
    """Meta tags plus the comment, for the <head> of any generated page."""
    return META_TAGS + HTML_COMMENT
