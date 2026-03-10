/**
 * Copyright (c) Meta Platforms, Inc. and affiliates.
 * All rights reserved.
 *
 * This source code is licensed under the BSD-style license found in the
 * LICENSE file in the root directory of this source tree.
 */

import React from "react";

interface Tab {
  id: string;
  label: string;
}

interface HeaderProps {
  tabs: Tab[];
  activeTab: string;
  onTabChange: (id: string) => void;
}

/** Monarch butterfly SVG logo. */
function ButterflyLogo() {
  return (
    <svg
      className="header-logo"
      viewBox="0 0 40 40"
      width="32"
      height="32"
      fill="none"
      aria-hidden="true"
    >
      {/* Left wing */}
      <path
        d="M20 20C16 12 6 6 4 10C2 14 8 22 14 24C10 18 12 14 20 20Z"
        fill="var(--accent-primary)"
        opacity="0.9"
      />
      <path
        d="M20 20C14 24 6 34 10 36C14 38 22 30 24 24C18 28 14 26 20 20Z"
        fill="var(--accent-secondary)"
        opacity="0.75"
      />
      {/* Right wing */}
      <path
        d="M20 20C24 12 34 6 36 10C38 14 32 22 26 24C30 18 28 14 20 20Z"
        fill="var(--accent-primary)"
        opacity="0.9"
      />
      <path
        d="M20 20C26 24 34 34 30 36C26 38 18 30 16 24C22 28 26 26 20 20Z"
        fill="var(--accent-secondary)"
        opacity="0.75"
      />
      {/* Body */}
      <ellipse cx="20" cy="20" rx="1.5" ry="6" fill="var(--text-primary)" />
      {/* Antennae */}
      <path d="M19 14Q16 8 14 6" stroke="var(--text-secondary)" strokeWidth="0.8" fill="none" />
      <path d="M21 14Q24 8 26 6" stroke="var(--text-secondary)" strokeWidth="0.8" fill="none" />
    </svg>
  );
}

/** Top header bar with Monarch branding and tab navigation. */
export function Header({ tabs, activeTab, onTabChange }: HeaderProps) {
  return (
    <header className="header">
      <div className="header-brand">
        <ButterflyLogo />
        <h1 className="header-title">Monarch</h1>
        <span className="header-subtitle">dashboard</span>
      </div>
      <nav className="header-tabs" role="tablist">
        {tabs.map((tab) => (
          <button
            key={tab.id}
            role="tab"
            aria-selected={tab.id === activeTab}
            className={`header-tab ${tab.id === activeTab ? "active" : ""}`}
            onClick={() => onTabChange(tab.id)}
          >
            {tab.label}
          </button>
        ))}
      </nav>
    </header>
  );
}
