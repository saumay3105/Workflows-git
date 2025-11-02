import React, { useState } from "react";

const DataDownloadPage = () => {
  const [downloading, setDownloading] = useState(false);
  const [error, setError] = useState(null);

  // Replace with your actual Azure Blob Storage URL
  const DOWNLOAD_URL = `https://saumayy.blob.core.windows.net/saumayycontainer/data_export.csv`;

  const handleDownload = async () => {
    setDownloading(true);
    setError(null);

    try {
      // Fetch the file from Azure Blob Storage
      const response = await fetch(DOWNLOAD_URL);

      if (!response.ok) {
        throw new Error(`Failed to download: ${response.statusText}`);
      }

      // Get the blob data
      const blob = await response.blob();

      // Create a download link
      const url = window.URL.createObjectURL(blob);
      const link = document.createElement("a");
      link.href = url;
      link.download = `data_export_${
        new Date().toISOString().split("T")[0]
      }.csv`;

      // Trigger download
      document.body.appendChild(link);
      link.click();

      // Cleanup
      document.body.removeChild(link);
      window.URL.revokeObjectURL(url);
    } catch (err) {
      console.error("Download error:", err);
      setError(err.message);
    } finally {
      setDownloading(false);
    }
  };

  return (
    <div style={styles.container}>
      <div style={styles.card}>
        <h1 style={styles.title}>Data Export Download</h1>
        <p style={styles.description}>
          Download the latest exported data from the database
        </p>

        <button
          onClick={handleDownload}
          disabled={downloading}
          style={{
            ...styles.button,
            ...(downloading ? styles.buttonDisabled : {}),
          }}
          onMouseEnter={(e) => {
            if (!downloading) {
              e.target.style.backgroundColor = "var(--color-primary-hover)";
            }
          }}
          onMouseLeave={(e) => {
            e.target.style.backgroundColor = "var(--color-primary)";
          }}
        >
          {downloading ? (
            <>
              <span style={styles.spinner}></span>
              Downloading...
            </>
          ) : (
            <>
              <svg
                style={styles.icon}
                fill="none"
                stroke="currentColor"
                viewBox="0 0 24 24"
              >
                <path
                  strokeLinecap="round"
                  strokeLinejoin="round"
                  strokeWidth={2}
                  d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4"
                />
              </svg>
              Download CSV
            </>
          )}
        </button>

        {error && (
          <div style={styles.error}>
            <svg
              style={styles.errorIcon}
              fill="currentColor"
              viewBox="0 0 20 20"
            >
              <path
                fillRule="evenodd"
                d="M10 18a8 8 0 100-16 8 8 0 000 16zM8.707 7.293a1 1 0 00-1.414 1.414L8.586 10l-1.293 1.293a1 1 0 101.414 1.414L10 11.414l1.293 1.293a1 1 0 001.414-1.414L11.414 10l1.293-1.293a1 1 0 00-1.414-1.414L10 8.586 8.707 7.293z"
                clipRule="evenodd"
              />
            </svg>
            {error}
          </div>
        )}

        <div style={styles.info}>
          <p style={styles.infoText}>📊 Data is updated daily at 2:00 AM UTC</p>
          <p style={styles.infoText}>
            📅 Last update: Check file timestamp after download
          </p>
        </div>
      </div>
    </div>
  );
};

const styles = {
  container: {
    minHeight: "100vh",
    display: "flex",
    alignItems: "center",
    justifyContent: "center",
    backgroundColor: "var(--color-background)",
    padding: "20px",
    fontFamily: "var(--font-family-base)",
  },
  card: {
    backgroundColor: "var(--color-surface)",
    borderRadius: "var(--radius-lg)",
    padding: "48px",
    maxWidth: "500px",
    width: "100%",
    boxShadow: "var(--shadow-lg)",
    border: "1px solid var(--color-card-border)",
    textAlign: "center",
  },
  title: {
    fontSize: "var(--font-size-3xl)",
    fontWeight: "var(--font-weight-bold)",
    color: "var(--color-text)",
    marginBottom: "12px",
  },
  description: {
    fontSize: "var(--font-size-base)",
    color: "var(--color-text-secondary)",
    marginBottom: "32px",
  },
  button: {
    display: "inline-flex",
    alignItems: "center",
    justifyContent: "center",
    gap: "8px",
    backgroundColor: "var(--color-primary)",
    color: "var(--color-btn-primary-text)",
    padding: "14px 32px",
    borderRadius: "var(--radius-base)",
    border: "none",
    fontSize: "var(--font-size-lg)",
    fontWeight: "var(--font-weight-medium)",
    cursor: "pointer",
    transition: "all var(--duration-normal) var(--ease-standard)",
    width: "100%",
  },
  buttonDisabled: {
    opacity: 0.6,
    cursor: "not-allowed",
  },
  icon: {
    width: "24px",
    height: "24px",
  },
  spinner: {
    width: "20px",
    height: "20px",
    border: "3px solid rgba(255, 255, 255, 0.3)",
    borderTop: "3px solid white",
    borderRadius: "50%",
    animation: "spin 1s linear infinite",
    display: "inline-block",
  },
  error: {
    marginTop: "24px",
    padding: "16px",
    backgroundColor: "rgba(var(--color-error-rgb), 0.1)",
    border: "1px solid var(--color-error)",
    borderRadius: "var(--radius-base)",
    color: "var(--color-error)",
    display: "flex",
    alignItems: "center",
    gap: "8px",
    fontSize: "var(--font-size-sm)",
  },
  errorIcon: {
    width: "20px",
    height: "20px",
    flexShrink: 0,
  },
  info: {
    marginTop: "32px",
    paddingTop: "24px",
    borderTop: "1px solid var(--color-border)",
  },
  infoText: {
    fontSize: "var(--font-size-sm)",
    color: "var(--color-text-secondary)",
    margin: "8px 0",
  },
};

// Add keyframe animation for spinner
const styleSheet = document.createElement("style");
styleSheet.textContent = `
  @keyframes spin {
    0% { transform: rotate(0deg); }
    100% { transform: rotate(360deg); }
  }
`;
document.head.appendChild(styleSheet);

export default DataDownloadPage;
