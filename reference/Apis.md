# Ticket Management System - API cURL Commands

## **User APIs**

### **1. Signup**

```bash
curl -X POST 'http://localhost:5000/api/users/signup' \
  --header 'Content-Type: application/json' \
  --data '{
    "name": "Pratik",
    "email": "pratik@test.com",
    "password": "StrongPass@123",
    "role": "user"
  }'
```

### **2. Login**

```bash
curl -X POST 'http://localhost:5000/api/users/login' \
  --header 'Content-Type: application/json' \
  --data '{
    "email": "pratik@test.com",
    "password": "StrongPass@123"
  }'
```

---

## **Ticket APIs**

### **1. Create Ticket**

```bash
curl -X POST 'http://localhost:5000/api/tickets' \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer YOUR_JWT_TOKEN' \
  --data '{
    "priority": "medium",
    "createdByUsername": "Pratik",
    "createdByUserEmail": "pratik@test.com",
    "createdByUserPhone": "1234567890",
    "IssueCategory": "Delivery Issue",
    "OrderId": "ORD-123456",
    "Subject": "Package delayed",
    "description": "My order has not arrived yet.",
    "AttachmentUrl": "",
    "InternalNotes": "",
    "replay": "",
    "replayAttachmentUrl": ""
  }'
```

### **2. Get Tickets for Logged-in Member**

```bash
curl -X GET 'http://localhost:5000/api/tickets/' \
  --header 'Authorization: Bearer YOUR_JWT_TOKEN'
```

### **3. Get All Tickets (Team Leader)**

```bash
curl -X GET 'http://localhost:5000/api/tickets/all' \
  --header 'Authorization: Bearer YOUR_JWT_TOKEN'
```

### **4. Reply to Ticket**

```bash
curl -X PUT 'http://localhost:5000/api/tickets/TICKET_ID' \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer YOUR_JWT_TOKEN' \
  --data '{
    "replay": "Issue resolved, please check.",
    "replayAttachmentUrl": ""
  }'
```

---

## **Issue Category APIs**

### **1. Create Category**

```bash
curl -X POST 'http://localhost:5000/api/categories' \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer YOUR_JWT_TOKEN' \
  --data '{
    "typeOfCategory": "Delivery Issue",
    "questions": "How long does delivery take?",
    "answer": "Usually 3-5 business days."
  }'
```

### **2. Get All Categories**

```bash
curl -X GET 'http://localhost:5000/api/categories' \
  --header 'Authorization: Bearer YOUR_JWT_TOKEN'
```

### **3. Update Category**

```bash
curl -X PUT 'http://localhost:5000/api/categories/CATEGORY_ID' \
  --header 'Content-Type: application/json' \
  --header 'Authorization: Bearer YOUR_JWT_TOKEN' \
  --data '{
    "typeOfCategory": "Payment Issue",
    "questions": "How to make payment?",
    "answer": "You can pay via credit/debit or UPI."
  }'
```

### **4. Delete Category**

```bash
curl -X DELETE 'http://localhost:5000/api/categories/CATEGORY_ID' \
  --header 'Authorization: Bearer YOUR_JWT_TOKEN'
```







SMTP password ( email services )----hwpd czuo svyd unzw




POST /api/categories


```bash
curl -X POST http://localhost:5000/api/categories \
  -H "Content-Type: application/json" \
  -d '{
    "typeOfCategory": "Payment Issue"
  }'

🔹 2. Get All Categories (With FAQs)

Endpoint

GET /api/categories


cURL

curl -X GET http://localhost:5000/api/categories

🔹 3. Get Category by Type

Endpoint

GET /api/categories/:type


cURL

curl -X GET http://localhost:5000/api/categories/Payment%20Issue

🔹 4. Update Category

Endpoint

PUT /api/categories/:type


cURL

curl -X PUT http://localhost:5000/api/categories/Payment%20Issue \
  -H "Content-Type: application/json" \
  -d '{
    "typeOfCategory": "Payment & Refund Issue"
  }'

🔹 5. Delete Category

Endpoint

DELETE /api/categories/:type


cURL

curl -X DELETE http://localhost:5000/api/categories/Payment%20Issue

🔹 6. Add FAQ to Category

Endpoint

POST /api/categories/:categoryId/faqs


cURL

curl -X POST http://localhost:5000/api/categories/1/faqs \
  -H "Content-Type: application/json" \
  -d '{
    "question": "My payment was deducted but order not placed",
    "answer": "If payment is deducted, it will be refunded within 3–5 working days."
  }'

🔹 7. Add Multiple FAQs to Same Category
curl -X POST http://localhost:5000/api/categories/1/faqs \
  -H "Content-Type: application/json" \
  -d '{
    "question": "Can I change payment method?",
    "answer": "Once payment is initiated, the payment method cannot be changed."
  }'

🔹 8. Update FAQ

Endpoint

PUT /api/faqs/:faqId


cURL

curl -X PUT http://localhost:5000/api/faqs/1 \
  -H "Content-Type: application/json" \
  -d '{
    "question": "Payment deducted but order not created",
    "answer": "Refund will be processed automatically within 3–5 business days."
  }'

🔹 9. Delete FAQ

Endpoint

DELETE /api/faqs/:faqId


cURL

curl -X DELETE http://localhost:5000/api/faqs/1