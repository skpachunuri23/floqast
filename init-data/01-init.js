db = db.getSiblingDB('accounting_db');

db.accounts.insertMany([
  { "account_id": "1000", "name": "Cash", "type": "Asset" },
  { "account_id": "1100", "name": "Accounts Receivable", "type": "Asset" },
  { "account_id": "2000", "name": "Accounts Payable", "type": "Liability" },
  { "account_id": "3000", "name": "Owner's Equity", "type": "Equity" },
  { "account_id": "4000", "name": "Revenue", "type": "Revenue" },
  { "account_id": "5000", "name": "Expenses", "type": "Expense" }
]);

db.journal_entries.insertMany([
  {
    "entry_id": "JE1001",
    "date": "2025-05-31",
    "lines": [
      { "account_id": "1000", "debit": 1000, "credit": 0 },
      { "account_id": "4000", "debit": 0, "credit": 1000 }
    ],
    "description": "Cash sales"
  },
  {
    "entry_id": "JE1002",
    "date": "2025-05-31",
    "lines": [
      { "account_id": "5000", "debit": 500, "credit": 0 },
      { "account_id": "1000", "debit": 0, "credit": 500 }
    ],
    "description": "Office supplies expense"
  },
  {
    "entry_id": "JE1003",
    "date": "2025-06-01",
    "lines": [
      { "account_id": "1100", "debit": 2000, "credit": 0 },
      { "account_id": "4000", "debit": 0, "credit": 2000 }
    ],
    "description": "Credit sales"
  }
]);

db.close_tasks.insertMany([
  { "task_id": "T1", "name": "Reconcile Cash", "assigned_to": "Alice", "status": "Complete", "due_date": "2025-06-05" },
  { "task_id": "T2", "name": "Review AP", "assigned_to": "Bob", "status": "In Progress", "due_date": "2025-06-06" },
  { "task_id": "T3", "name": "Prepare Financial Statements", "assigned_to": "Carol", "status": "Not Started", "due_date": "2025-06-10" }
]);

print("Sample data inserted successfully!");
