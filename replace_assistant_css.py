import sys, pathlib
sys.stdout.reconfigure(encoding='utf-8')

css_path = pathlib.Path(r'c:/Users/Caron/OneDrive/Desktop/Agent SEO IA/seo-agent-web/static/styles.css')
content = css_path.read_text(encoding='utf-8')
lines = content.split('\n')

# Find boundaries
start_line = None
end_line = None
for i, line in enumerate(lines):
    if 'AI Assistant' in line and start_line is None:
        start_line = i - 1
    if 'Misc UI components' in line and end_line is None:
        end_line = i - 1

print(f'Replacing lines {start_line}–{end_line}')

new_section = r"""/* ═══════════════════════════════════════════════════════
   AI Assistant
═══════════════════════════════════════════════════════ */
.assistant-dock {
  position: fixed;
  right: 22px;
  bottom: 22px;
  z-index: 50;
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 12px;
  max-width: calc(100vw - 36px);
}

/* Round FAB launcher */
.assistant-launcher {
  appearance: none;
  border: none;
  width: 54px;
  height: 54px;
  border-radius: 50%;
  background: linear-gradient(145deg, rgba(var(--accent-rgb), .95), rgba(var(--accent-rgb), .60));
  box-shadow: 0 4px 20px rgba(var(--accent-rgb), .40), 0 2px 8px rgba(0,0,0,.45);
  cursor: pointer;
  display: grid;
  place-items: center;
  padding: 0;
  position: relative;
  transition: transform .15s ease, box-shadow .15s ease;
  flex: 0 0 auto;
}
.assistant-launcher:hover {
  transform: translateY(-2px);
  box-shadow: 0 8px 28px rgba(var(--accent-rgb), .52), 0 2px 8px rgba(0,0,0,.45);
}
.assistant-launcher:active { transform: translateY(0); }
.assistant-launcher-text { display: none; }

.assistant-launcher-icon {
  width: 28px;
  height: 28px;
  display: grid;
  place-items: center;
  flex: 0 0 auto;
}
.assistant-head-icon {
  width: 36px;
  height: 36px;
  border-radius: 10px;
  border: 1px solid rgba(var(--accent-rgb), .28);
  background: rgba(var(--accent-rgb), .12);
  display: grid;
  place-items: center;
  flex: 0 0 auto;
}
.assistant-icon-img { width: 22px; height: 22px; display: block; }
.assistant-launcher .assistant-icon-img { width: 27px; height: 27px; filter: brightness(1.2); }

.assistant-fab-dot {
  position: absolute;
  top: 2px;
  right: 2px;
  width: 11px;
  height: 11px;
  border-radius: 50%;
  background: rgba(255,255,255,.18);
  border: 2px solid rgba(13,13,16,.9);
}
.assistant-fab-dot.ok  { background: #22c55e; }
.assistant-fab-dot.err { background: var(--err); }

/* Panel */
.assistant-panel {
  position: absolute;
  right: 0;
  bottom: calc(100% + 14px);
  width: 400px;
  max-width: calc(100vw - 36px);
  max-height: min(80vh, 620px);
  background: rgba(13, 13, 16, .98);
  border: 1px solid rgba(var(--accent-rgb), .16);
  border-radius: 20px;
  box-shadow: 0 16px 48px rgba(0,0,0,.65), 0 0 0 1px rgba(255,255,255,.04);
  overflow: hidden;
  display: flex;
  flex-direction: column;
  backdrop-filter: blur(20px);
  opacity: 0;
  transform: translateY(12px) scale(.97);
  pointer-events: none;
  transition: opacity .18s ease, transform .18s cubic-bezier(.34,1.1,.64,1);
}
.assistant-dock.assistant-open .assistant-panel {
  opacity: 1;
  transform: none;
  pointer-events: auto;
}
.assistant-panel::before {
  content: "";
  position: absolute;
  top: 0; left: 0; right: 0;
  height: 2px;
  background: linear-gradient(90deg, rgba(var(--accent-rgb), .85) 0%, rgba(var(--accent-rgb), .25) 65%, transparent 100%);
  z-index: 3;
  pointer-events: none;
  border-radius: 20px 20px 0 0;
}

.assistant-head {
  position: relative;
  z-index: 1;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  padding: 13px 14px;
  border-bottom: 1px solid rgba(var(--accent-rgb), .10);
  background: rgba(var(--accent-rgb), .05);
}
.assistant-head-left  { display: flex; align-items: center; gap: 10px; min-width: 0; }
.assistant-head-text  { min-width: 0; }
.assistant-title      { font-weight: 800; font-size: 13px; letter-spacing: .01em; }
.assistant-meta       { color: var(--muted); font-size: 11px; margin-top: 2px; }
.assistant-head-actions { display: inline-flex; align-items: center; gap: 6px; }

.assistant-clear,
.assistant-close {
  appearance: none;
  border: 1px solid rgba(255,255,255,.08);
  background: transparent;
  color: var(--muted);
  border-radius: 9px;
  width: 30px;
  height: 30px;
  cursor: pointer;
  display: grid;
  place-items: center;
  transition: background .12s ease, color .12s ease, border-color .12s ease;
}
.assistant-clear:hover,
.assistant-close:hover {
  background: rgba(255,255,255,.08);
  color: var(--text);
  border-color: rgba(255,255,255,.15);
}
.assistant-clear svg,
.assistant-close svg { width: 15px; height: 15px; }

.assistant-hero { display: none; }

/* Messages */
.assistant-messages {
  position: relative;
  z-index: 1;
  padding: 16px 14px;
  overflow: auto;
  display: flex;
  flex-direction: column;
  gap: 10px;
  flex: 1 1 auto;
  scroll-behavior: smooth;
}
.assistant-msg { display: flex; flex-direction: column; gap: 4px; max-width: 100%; }
.assistant-msg.user      { align-items: flex-end; }
.assistant-msg.assistant { align-items: flex-start; }
.assistant-msg .assistant-role { display: none; }
.assistant-msg .assistant-bubble {
  white-space: pre-wrap;
  word-break: break-word;
  padding: 10px 13px;
  font-size: 13px;
  line-height: 1.5;
  max-width: 86%;
  border: 1px solid rgba(255,255,255,.07);
  background: rgba(255,255,255,.04);
  color: var(--text);
}
.assistant-msg.user .assistant-bubble {
  background: rgba(var(--accent-rgb), .18);
  border-color: rgba(var(--accent-rgb), .30);
  border-radius: 16px 16px 4px 16px;
}
.assistant-msg.assistant .assistant-bubble {
  border-radius: 4px 16px 16px 16px;
  background: rgba(255,255,255,.045);
}
.assistant-msg.error .assistant-bubble {
  background: rgba(239,68,68,.10);
  border-color: rgba(239,68,68,.25);
  border-radius: 4px 16px 16px 16px;
}

/* Form */
.assistant-form {
  position: relative;
  z-index: 1;
  display: flex;
  gap: 8px;
  padding: 10px 12px 14px;
  border-top: 1px solid rgba(var(--accent-rgb), .08);
  background: rgba(0,0,0,.15);
}
.assistant-input {
  flex: 1;
  border-radius: 14px;
  height: 44px;
  padding: 0 14px;
  font-size: 13.5px;
  border: 1px solid rgba(var(--accent-rgb), .15);
  background: rgba(255,255,255,.04);
  color: var(--text);
  transition: border-color .12s ease, box-shadow .12s ease;
}
.assistant-input:focus {
  border-color: rgba(var(--accent-rgb), .40);
  outline: none;
  box-shadow: 0 0 0 3px rgba(var(--accent-rgb), .10);
}
.assistant-send {
  flex: 0 0 auto;
  appearance: none;
  width: 44px;
  height: 44px;
  border-radius: 14px;
  border: none;
  background: linear-gradient(145deg, rgba(var(--accent-rgb), .92), rgba(var(--accent-rgb), .65));
  color: #fff;
  cursor: pointer;
  display: grid;
  place-items: center;
  padding: 0;
  box-shadow: 0 2px 8px rgba(var(--accent-rgb), .30);
  transition: transform .12s ease, box-shadow .12s ease;
}
.assistant-send svg { width: 16px; height: 16px; }
.assistant-send:hover {
  transform: translateY(-1px);
  box-shadow: 0 4px 14px rgba(var(--accent-rgb), .42);
}
.assistant-send:disabled { opacity: .45; cursor: not-allowed; transform: none; }
.assistant-foot { position: relative; z-index: 1; padding: 0 14px 10px; font-size: 11px; }

/* Typing animation */
.assistant-typing {
  display: inline-flex !important;
  align-items: center;
  gap: 5px;
  padding: 12px 15px !important;
  min-width: 58px;
}
.assistant-typing span {
  width: 6px;
  height: 6px;
  border-radius: 50%;
  background: var(--muted);
  animation: assistant-bounce 1.1s ease infinite;
  display: block;
  flex: 0 0 auto;
}
.assistant-typing span:nth-child(2) { animation-delay: .18s; }
.assistant-typing span:nth-child(3) { animation-delay: .36s; }
@keyframes assistant-bounce {
  0%, 75%, 100% { transform: translateY(0); opacity: .35; }
  35%            { transform: translateY(-5px); opacity: 1; }
}

/* Empty state */
.assistant-empty {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  gap: 10px;
  padding: 24px 20px 16px;
  text-align: center;
}
.assistant-empty-icon {
  width: 68px;
  height: 68px;
  border-radius: 20px;
  background: rgba(var(--accent-rgb), .10);
  border: 1px solid rgba(var(--accent-rgb), .22);
  display: grid;
  place-items: center;
  margin-bottom: 4px;
  box-shadow: 0 0 24px rgba(var(--accent-rgb), .12);
}
.assistant-empty-icon img { width: 38px; height: 38px; }
.assistant-empty-title { font-weight: 700; font-size: 14px; }
.assistant-empty-sub {
  font-size: 12px;
  color: var(--muted);
  line-height: 1.5;
  max-width: 260px;
  margin-bottom: 4px;
}
.assistant-suggestions {
  display: flex;
  flex-direction: column;
  gap: 7px;
  width: 100%;
  max-width: 310px;
}
.assistant-suggestion {
  appearance: none;
  border: 1px solid rgba(255,255,255,.09);
  background: rgba(255,255,255,.03);
  color: var(--text);
  border-radius: 12px;
  padding: 9px 14px;
  font-size: 12.5px;
  text-align: left;
  cursor: pointer;
  transition: background .12s ease, border-color .12s ease;
  line-height: 1.4;
}
.assistant-suggestion:hover {
  background: rgba(var(--accent-rgb), .10);
  border-color: rgba(var(--accent-rgb), .35);
  color: var(--text);
}

/* Markdown in bubbles */
.assistant-bubble p { margin: 0 0 6px; }
.assistant-bubble p:last-child { margin-bottom: 0; }
.assistant-bubble ul,
.assistant-bubble ol { margin: 4px 0 6px; padding-left: 18px; }
.assistant-bubble li { margin: 2px 0; line-height: 1.5; }
.assistant-bubble strong { font-weight: 700; }
.assistant-bubble code {
  font-family: ui-monospace, monospace;
  font-size: .88em;
  background: rgba(255,255,255,.08);
  border-radius: 4px;
  padding: 1px 5px;
}

@media (prefers-reduced-motion: reduce) {
  .assistant-panel { transition: none; }
  .assistant-typing span { animation: none; opacity: .5; }
}

@media (max-width: 740px) {
  .assistant-dock  { right: 14px; bottom: 14px; }
  .assistant-panel { width: calc(100vw - 28px); right: -8px; }
}

"""

new_lines = lines[:start_line] + [new_section] + lines[end_line:]
new_content = '\n'.join(new_lines)
css_path.write_text(new_content, encoding='utf-8')
print(f'Done. New total chars: {len(new_content)}')
