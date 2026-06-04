import type { ReactNode } from 'react';
import type { BannerVariant } from '../types';

export function StatusBanner({
  variant,
  alert,
  className,
  children,
}: {
  variant: BannerVariant;
  alert?: boolean;
  className?: string;
  children: ReactNode;
}) {
  const classes = ['banner', `banner--${variant}`, className].filter(Boolean).join(' ');
  return (
    <div role={alert ? 'alert' : undefined} className={classes}>
      {children}
    </div>
  );
}
