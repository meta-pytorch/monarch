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

/** Official torch-monarch logo. */
function MonarchLogo() {
  return (
    <svg
      className="header-logo"
      viewBox="0 0 171 170"
      width="32"
      height="32"
      fill="none"
      aria-hidden="true"
    >
      <g clipPath="url(#monarch-logo-clip)">
        <path
          d="M87.7837 115.185L20.5159 119.007C14.6855 119.339 10.6965 114.477 10.9063 109.489C11.0701 107.885 11.583 106.326 12.3997 104.94C12.6864 104.512 13.0159 104.095 13.3912 103.696L15.1595 101.817C16.7859 100.574 18.8198 99.7661 21.1686 99.6374L95.9088 95.5456L87.7837 115.185ZM107.124 4.08886C116.809 -6.20282 133.528 4.623 128.123 17.6864L102.076 80.6412L31.4477 84.5075L107.124 4.08886Z"
          fill="#FDBD97"
        />
        <path
          d="M14.0932 118.284C7.37588 111.629 11.727 100.154 21.1636 99.6372L149.005 92.6394C159.639 92.0573 164.707 105.52 156.335 112.109L88.7152 165.328C80.0742 172.128 67.727 171.423 59.9146 163.683L14.0932 118.284Z"
          fill="#EC6C46"
        />
      </g>
      <defs>
        <clipPath id="monarch-logo-clip">
          <rect width="170" height="170" fill="white" transform="translate(0.84375)" />
        </clipPath>
      </defs>
    </svg>
  );
}

/** Top header bar with Monarch branding and tab navigation. */
export function Header({ tabs, activeTab, onTabChange }: HeaderProps) {
  return (
    <header className="header">
      <div className="header-brand">
        <MonarchLogo />
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
