import api from './index'

export const getCapacityStatus = () => api.get('/metrics/status')
export const evaluateCapacityAlerts = () => api.post('/metrics/alerts/evaluate')
