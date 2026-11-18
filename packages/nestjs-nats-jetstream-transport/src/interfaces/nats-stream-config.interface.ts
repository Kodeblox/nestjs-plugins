/**
 * Driver-neutral stream configuration (spec 001, section 4.2.3). Named
 * fields cover the commonly used knobs with their wire types; anything else
 * rides the index signature and is forwarded to `streams.add` / `update`
 * untouched. The published `.d.ts` never references a driver package.
 */
export interface NatsStreamConfig {
  name: string;
  subjects: string[];
  storage?: string;
  retention?: string;
  discard?: string;
  max_msgs?: number;
  max_msgs_per_subject?: number;
  max_msg_size?: number;
  max_age?: number;
  duplicate_window?: number;
  num_replicas?: number;
  max_bytes?: number;
  max_consumers?: number;
  description?: string;
  sealed?: boolean;
  deny_delete?: boolean;
  deny_purge?: boolean;
  allow_rollup_hdrs?: boolean;
  allow_direct?: boolean;
  discard_new_per_subject?: boolean;
  mirror_direct?: boolean;
  no_ack?: boolean;
  compression?: string;
  first_seq?: number;
  placement?: unknown;
  mirror?: unknown;
  sources?: unknown;
  metadata?: Record<string, string>;
  republish?: unknown;
  subject_transform?: unknown;
  consumer_limits?: unknown;
  /** Pass-through for any other stream option. */
  [option: string]: unknown;
}
