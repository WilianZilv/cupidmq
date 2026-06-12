import { useEffect, useRef } from "react";
import { HELP_SECTIONS } from "./metricsHelp";

interface Props {
  open: boolean;
  onClose: () => void;
}

export function HelpDialog({ open, onClose }: Props) {
  const dialogRef = useRef<HTMLDialogElement>(null);

  useEffect(() => {
    const el = dialogRef.current;
    if (!el) return;
    if (open && !el.open) el.showModal();
    if (!open && el.open) el.close();
  }, [open]);

  return (
    <dialog
      ref={dialogRef}
      className="help-dialog"
      onClose={onClose}
      aria-labelledby="help-dialog-title"
    >
      <header className="help-dialog-head">
        <h2 id="help-dialog-title">CupidMQ guide</h2>
        <button
          type="button"
          className="help-dialog-close"
          aria-label="Close help"
          onClick={onClose}
        >
          ×
        </button>
      </header>
      <div className="help-dialog-body">
        {HELP_SECTIONS.map((sec) => (
          <section key={sec.title} className="help-section">
            <h3>{sec.title}</h3>
            <p>{sec.body.split("\n\n").join("\n\n")}</p>
          </section>
        ))}
      </div>
      <footer className="help-dialog-foot">
        <button type="button" className="help-dialog-ok" onClick={onClose}>
          Close
        </button>
      </footer>
    </dialog>
  );
}
