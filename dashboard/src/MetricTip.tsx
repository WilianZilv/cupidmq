import type { CSSProperties, ReactNode } from "react";

interface Props {
  /** Texto curto — aria-label + tooltip nativo no hover. */
  label: string;
  children: ReactNode;
  className?: string;
  style?: CSSProperties;
  as?: "span" | "strong" | "div";
}

export function MetricTip({
  label,
  children,
  className,
  style,
  as: Tag = "span",
}: Props) {
  return (
    <Tag
      className={["metric-tip", className].filter(Boolean).join(" ")}
      aria-label={label}
      title={label}
      style={style}
    >
      {children}
    </Tag>
  );
}
