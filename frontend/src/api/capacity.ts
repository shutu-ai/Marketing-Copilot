import api from './index'

export const getCapacityStatus = () => api.get('/metrics/status')
export const getCampaignCapacity = (limit = 200) => api.get('/capacity/campaigns', { params: { limit } })
export const evaluateCapacityAlerts = () => api.post('/metrics/alerts/evaluate')
export const getSmppStats = () => api.get('/smpp/stats')
