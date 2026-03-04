# Backend Migration Notes: Canonical Metrics & Limit Orders

## Database schema changes

A new `limit_order` table is introduced through SQLAlchemy `db.create_all()` with these fields:

- `id` (PK)
- `user_id` (FK -> `user.id`)
- `symbol`
- `side`
- `quantity`
- `limit_price`
- `created_at`
- `updated_at`
- `status` (`open`, `partially_filled`, `filled`, `cancelled`, `expired`, `rejected`)
- `account_context`
- `filled_qty`
- `avg_fill_price`

## API contract updates

### Canonical stock metrics

- `GET /stock_overview/:symbol?range=1D|1W|1M|6M|1Y`
  - Returns canonical current/prev close/day change/range change and chart points.
  - Frontend should use these values directly and stop computing day/range metrics client-side.

### Limit orders

- `GET /orders/limit?username=:username&status=open`
- `POST /orders/limit`
- `POST /orders/limit/:id/cancel`

Open and historical views are supported via optional `status` query filtering.

## Backward compatibility

- Existing endpoints (`/stock/:symbol`, `/stock_chart/:symbol`, `/buy`, `/sell`) remain available.
- `/stock_chart/:symbol` now uses the same canonical data pipeline as `/stock_overview/:symbol` to keep chart behavior consistent.
- Frontend migration can be incremental: begin reading `/stock_overview` first while legacy routes stay online.

## Operational behavior

- A background scheduler evaluates open limit orders on an interval.
- Order execution checks run server-side, independent of user login state.
- Logging now includes provider snapshot timestamps and metric inputs/outputs for audit.
